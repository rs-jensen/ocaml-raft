open Sim
module N = Sim.Node

let quiet = { defaults with drop = 0.; faults = 0.; load = 0.; reads = 0.; changes = false }

let check msg b = if not b then failwith msg

let wait ?(limit = 3000) s f =
  let n = ref 0 in
  while not (f ()) do
    if !n >= limit then failwith "timed out";
    step ~faults:false ~load:false s;
    incr n
  done

let idle s ticks =
  for _ = 1 to ticks do
    step ~faults:false ~load:false s
  done

let elect s =
  wait s (fun () -> leader s <> None);
  Option.get (leader s)

let elect_ready s =
  let l = elect s in
  wait s (fun () -> Node.commit_index s.nodes.(l) = Node.last_index s.nodes.(l));
  l

let settled s = (not (Node.change_pending s.nodes.(Option.get (leader s)))) && converged s
let same_state a b = Kv.equal (Node.state a) (Node.state b)

let entry t i = { N.term = t; index = i; data = N.Noop }

let empty_snap members =
  { N.snap_index = 0; snap_term = 0; snap_members = members; snap_state = Kv.empty }

let log_basics () =
  let l = List.fold_left N.Log.add (N.Log.create (empty_snap [ 1 ])) [ entry 1 1; entry 1 2; entry 2 3; entry 2 4 ] in
  check "last" (N.Log.last l = 4);
  check "last term" (N.Log.last_term l = 2);
  check "range" (List.map (fun e -> e.N.index) (N.Log.range l 2 3) = [ 2; 3 ]);
  check "first of term" (N.Log.first_of_term l 4 = 3);
  check "last of term" (N.Log.last_of_term l 1 = Some 2);
  check "missing term" (N.Log.last_of_term l 5 = None);
  let same = N.Log.merge l [ entry 1 1; entry 1 2 ] in
  check "matching prefix keeps tail" (N.Log.last same = 4);
  let fixed = N.Log.merge l [ entry 1 2; entry 3 3 ] in
  check "conflict cuts" (N.Log.last fixed = 3);
  check "conflict replaced" (N.Log.term fixed 3 = Some 3);
  check "up to date" (N.Log.up_to_date l 4 2 && N.Log.up_to_date l 1 3);
  check "not up to date" (not (N.Log.up_to_date l 5 1) && not (N.Log.up_to_date l 3 2))

let log_compaction () =
  let l = List.fold_left N.Log.add (N.Log.create (empty_snap [ 1 ])) [ entry 1 1; entry 1 2; entry 2 3 ] in
  let snap = { N.snap_index = 2; snap_term = 1; snap_members = [ 1 ]; snap_state = Kv.empty } in
  let c = N.Log.compact l snap in
  check "base" (N.Log.base c = 2);
  check "old entry gone" (N.Log.get c 1 = None);
  check "snapshot term" (N.Log.term c 2 = Some 1);
  check "tail kept" (N.Log.term c 3 = Some 2 && N.Log.last c = 3);
  let kept = N.Log.install l snap in
  check "install keeps matching tail" (N.Log.last kept = 3);
  let other = { snap with snap_index = 3; snap_term = 5 } in
  let reset = N.Log.install l other in
  check "install drops conflicting log" (N.Log.last reset = 3 && N.Log.get reset 3 = None)

let log_members () =
  let l = N.Log.create (empty_snap [ 1; 2; 3 ]) in
  let l = N.Log.add l (entry 1 1) in
  let l = N.Log.add l { N.term = 1; index = 2; data = N.Members [ 1; 2; 3; 4 ] } in
  let l = N.Log.add l (entry 1 3) in
  check "latest config" (N.Log.members l = [ 1; 2; 3; 4 ]);
  check "config at index" (N.Log.members_at l 1 = [ 1; 2; 3 ]);
  check "last change" (N.Log.last_change l = 2);
  let cut = N.Log.merge l [ entry 2 2 ] in
  check "cut reverts config" (N.Log.members cut = [ 1; 2; 3 ]);
  let snap = { N.snap_index = 3; snap_term = 1; snap_members = [ 1; 2; 3; 4 ]; snap_state = Kv.empty } in
  check "config survives compaction" (N.Log.members (N.Log.compact l snap) = [ 1; 2; 3; 4 ])

let config_checks () =
  let raises f =
    match f () with
    | _ -> false
    | exception Invalid_argument _ -> true
  in
  check "id outside" (raises (fun () -> Raft.config ~members:[ 1; 2 ] 3));
  check "heartbeat too slow" (raises (fun () -> Raft.config ~heartbeat_ticks:10 ~members:[ 1 ] 1));
  check "heartbeat zero" (raises (fun () -> Raft.config ~heartbeat_ticks:0 ~members:[ 1 ] 1));
  check "joining node is fine" (not (raises (fun () -> Raft.config ~members:[] 7)));
  let c = Raft.config ~members:[ 3; 1; 2; 1 ] 1 in
  check "members sorted" (c.members = [ 1; 2; 3 ])

let stale_vote_gets_term () =
  let n = N.create (Raft.config ~members:[ 0; 1; 2 ] 0) in
  let hello = N.Append { term = 5; prev_index = 0; prev_term = 0; entries = []; commit = 0; round = 0 } in
  let n, _ = N.receive n ~from:1 hello in
  check "follows leader" (N.term n = 5 && N.leader n = Some 1);
  let ask = N.Vote_req { term = 3; last_index = 9; last_term = 3; pre = false; forced = false } in
  let _, out = N.receive n ~from:2 ask in
  match out with
  | [ N.Send (2, N.Vote_resp r) ] -> check "reply has new term" (r.term = 5 && not r.granted)
  | _ -> failwith "expected one vote reply"

let joiner_waits () =
  let n = ref (N.create (Raft.config ~members:[] 9)) in
  for _ = 1 to 300 do
    let m, out = N.tick !n in
    n := m;
    check "joiner sent something" (out = [])
  done;
  check "joiner still follower" (N.role !n = N.Follower && N.term !n = 0)

let single_node () =
  let s = create { quiet with size = 1 } in
  check "leader" (elect s = 0);
  check "submit" (submit s);
  wait s (fun () -> s.stats.confirmed = 1);
  check "read" (read s);
  check "read answered right away" (s.stats.reads_done = 1)

let one_leader () =
  let s = create { quiet with seed = 3 } in
  let l = elect s in
  idle s 500;
  let leaders = List.filter (fun n -> Node.role n = Node.Leader) (Array.to_list s.nodes) in
  check "one leader" (List.length leaders = 1);
  check "same leader" (leader s = Some l)

let replication () =
  let s = create { quiet with seed = 11 } in
  let l = elect s in
  for _ = 1 to 40 do
    check "submit" (submit ~at:l s);
    idle s 1
  done;
  wait s (fun () -> converged s);
  check "all confirmed" (s.stats.confirmed = 40);
  Array.iter (fun n -> check "same state" (same_state s.nodes.(0) n)) s.nodes

let isolated_leader () =
  let s = create { quiet with seed = 4 } in
  let old = elect s in
  let term = Node.term s.nodes.(old) in
  partition s [ [ old ] ];
  let lost = s.next_cmd in
  check "old leader takes write" (submit ~at:old s);
  wait s (fun () -> Node.role s.nodes.(old) <> Node.Leader);
  wait s (fun () -> match leader s with Some l -> l <> old | None -> false);
  let fresh = Option.get (leader s) in
  check "newer term" (Node.term s.nodes.(fresh) > term);
  check "new leader takes write" (submit ~at:fresh s);
  heal s;
  wait s (fun () -> converged s);
  check "lost write gone" (not (Hashtbl.mem s.placed lost));
  check "one write kept" (Hashtbl.length s.placed = 1)

let minority_stalls () =
  let s = create { quiet with seed = 8 } in
  let l = elect s in
  let others = List.filter (fun id -> id <> l) (List.init 5 (fun i -> i)) in
  List.iteri (fun i id -> if i < 3 then crash s id) others;
  let before = Node.commit_index s.nodes.(l) in
  ignore (submit ~at:l s);
  idle s 200;
  check "nothing committed" (Node.commit_index s.nodes.(l) = before);
  List.iter (restart s) others;
  wait s (fun () -> converged s);
  check "write done or dropped" (s.stats.confirmed + Hashtbl.length s.waiting = 1)

let transfer () =
  let s = create { quiet with size = 3; seed = 5 } in
  let old = elect s in
  let target = (old + 1) mod 3 in
  for _ = 1 to 10 do
    ignore (submit s)
  done;
  hand_over s old target;
  check "no writes during transfer" (not (submit ~at:old s));
  wait s (fun () -> leader s = Some target);
  check "old is follower" (Node.role s.nodes.(old) = Node.Follower)

let snapshot_catch_up () =
  let s = create { quiet with size = 3; seed = 6; compact_after = 16 } in
  let l = elect s in
  let lag = (l + 1) mod 3 in
  crash s lag;
  for _ = 1 to 120 do
    ignore (submit s);
    idle s 1
  done;
  check "leader compacted" (Node.snapshot_index s.nodes.(l) > 0);
  restart s lag;
  wait s (fun () -> converged s);
  check "snapshot sent" (s.stats.snapshots > 0);
  check "lagging node has snapshot" (Node.snapshot_index s.nodes.(lag) > 0);
  check "same state" (same_state s.nodes.(lag) s.nodes.(l))

let restart_keeps_state () =
  let s = create { quiet with size = 3; seed = 9; compact_after = 8 } in
  ignore (elect s);
  for _ = 1 to 30 do
    ignore (submit s);
    idle s 1
  done;
  wait s (fun () -> converged s);
  let before = Node.state s.nodes.(0) in
  List.iter (crash s) [ 0; 1; 2 ];
  idle s 5;
  List.iter (restart s) [ 0; 1; 2 ];
  ignore (elect s);
  wait s (fun () -> converged s);
  Array.iter (fun n -> check "state kept" (Kv.equal before (Node.state n))) s.nodes

let read_after_write () =
  let s = create { quiet with seed = 12 } in
  let l = elect s in
  for _ = 1 to 20 do
    ignore (submit ~at:l s)
  done;
  wait s (fun () -> s.stats.confirmed = 20);
  check "read" (read ~at:l s);
  wait s (fun () -> s.stats.reads_done = 1);
  let follower = (l + 1) mod 5 in
  match Node.read s.nodes.(follower) with
  | Error (Some x) -> check "follower points to leader" (x = l)
  | _ -> failwith "follower answered a read"

let many_reads () =
  let s = create { quiet with seed = 13 } in
  let l = elect s in
  for _ = 1 to 50 do
    check "read" (read ~at:l s)
  done;
  wait s (fun () -> s.stats.reads_done = 50)

let stale_leader_read () =
  let s = create { quiet with seed = 14; check_quorum = false } in
  let old = elect s in
  idle s 20;
  partition s [ [ old ] ];
  check "old leader takes read" (read ~at:old s);
  wait s (fun () -> match leader s with Some l -> l <> old | None -> false);
  let fresh = Option.get (leader s) in
  check "write" (submit ~at:fresh s);
  wait s (fun () -> s.stats.confirmed = 1);
  idle s 100;
  check "old leader still thinks it leads" (Node.role s.nodes.(old) = Node.Leader);
  check "stale read not answered" (s.stats.reads_done = 0);
  heal s;
  wait s (fun () -> Node.role s.nodes.(old) <> Node.Leader);
  idle s 100;
  check "dropped after step down" (s.stats.reads_done = 0)

let add_nodes () =
  let s = create { quiet with size = 5; spare = 2; seed = 15 } in
  let l = elect_ready s in
  check "three members" (Node.members s.nodes.(l) = [ 0; 1; 2 ]);
  for _ = 1 to 10 do
    ignore (submit ~at:l s)
  done;
  check "add 3" (add_node s ~via:l 3);
  check "only one change at a time" (not (add_node s ~via:l 4));
  wait s (fun () -> not (Node.change_pending s.nodes.(l)));
  check "add 4" (add_node s ~via:l 4);
  wait s (fun () -> settled s);
  Array.iter (fun n -> check "five members" (Node.members n = [ 0; 1; 2; 3; 4 ])) s.nodes;
  Array.iter (fun n -> check "same state" (same_state s.nodes.(l) n)) s.nodes

let bad_changes () =
  let s = create { quiet with size = 3; seed = 16 } in
  let l = elect s in
  wait s (fun () -> Node.commit_index s.nodes.(l) > 0);
  let follower = (l + 1) mod 3 in
  check "follower can not change" (Result.is_error (Node.add_member s.nodes.(follower) 7));
  check "already in" (Result.is_error (Node.add_member s.nodes.(l) follower));
  check "not in" (Result.is_error (Node.remove_member s.nodes.(l) 9));
  check "two at once" (Result.is_error (Node.change_members s.nodes.(l) [ l ]));
  let solo = create { quiet with size = 1 } in
  ignore (elect solo);
  check "can not empty" (Result.is_error (Node.remove_member solo.nodes.(0) 0))

let remove_follower () =
  let s = create { quiet with seed = 17 } in
  let l = elect_ready s in
  let f = (l + 2) mod 5 in
  check "remove" (remove_node s ~via:l f);
  wait s (fun () -> s.retired.(f));
  check "four left" (List.length (Node.members s.nodes.(l)) = 4);
  check "write" (submit ~at:l s);
  wait s (fun () -> s.stats.confirmed = 1);
  idle s 300;
  check "removed node quiet" (Node.role s.nodes.(f) <> Node.Leader);
  check "same leader" (leader s = Some l)

let remove_leader () =
  let s = create { quiet with seed = 18 } in
  let l = elect_ready s in
  for _ = 1 to 5 do
    ignore (submit ~at:l s)
  done;
  check "remove self" (remove_node s ~via:l l);
  wait s (fun () -> match leader s with Some x -> x <> l | None -> false);
  let fresh = Option.get (leader s) in
  check "old leader gone" (not (List.mem l (Node.members s.nodes.(fresh))));
  wait s (fun () -> s.retired.(l));
  check "write" (submit ~at:fresh s);
  wait s (fun () -> s.stats.confirmed = 6)

let remove_and_add_back () =
  let s = create { quiet with size = 3; seed = 19; compact_after = 8 } in
  let l = elect_ready s in
  let f = (l + 1) mod 3 in
  check "remove" (remove_node s ~via:l f);
  wait s (fun () -> s.retired.(f));
  for _ = 1 to 40 do
    ignore (submit ~at:l s);
    idle s 1
  done;
  check "add back" (add_node s ~via:l f);
  wait s (fun () -> settled s);
  check "got snapshot" (s.stats.snapshots > 0);
  check "same state" (same_state s.nodes.(f) s.nodes.(l))

let same_seed_same_run () =
  let p = { defaults with seed = 21; steps = 2000 } in
  let a = run p and b = run p in
  check "same stats" (a.stats = b.stats);
  check "same state" (same_state a.nodes.(0) b.nodes.(0))

let chaos () =
  let configs =
    [
      { defaults with size = 3 };
      { defaults with drop = 0.1; batch = 2; delay = 6 };
      { defaults with size = 7; compact_after = 8; spare = 2 };
      { defaults with size = 4; pre_vote = false };
      { defaults with check_quorum = false; batch = 1; reads = 0.5 };
      { defaults with pre_vote = false; check_quorum = false; drop = 0.1 };
      { defaults with size = 6; spare = 3; compact_after = 4 };
      { defaults with size = 2 };
    ]
  in
  let attempt p =
    match run p with
    | _ -> None
    | exception Violation msg -> Some (Printf.sprintf "%d nodes, seed %d: %s" p.size p.seed msg)
  in
  let errors =
    List.concat_map
      (fun p ->
        List.init 25 (fun i -> { p with seed = i + 1; steps = 3000; faults = 0.03 })
        |> List.filter_map attempt)
      configs
  in
  if errors <> [] then failwith (String.concat "\n" errors)

let () =
  let tests =
    [
      ("log basics", log_basics);
      ("log compaction", log_compaction);
      ("log members", log_members);
      ("config checks", config_checks);
      ("stale vote gets term", stale_vote_gets_term);
      ("joiner waits", joiner_waits);
      ("single node", single_node);
      ("one leader", one_leader);
      ("replication", replication);
      ("isolated leader", isolated_leader);
      ("minority stalls", minority_stalls);
      ("leadership transfer", transfer);
      ("snapshot catch up", snapshot_catch_up);
      ("restart keeps state", restart_keeps_state);
      ("read after write", read_after_write);
      ("many reads", many_reads);
      ("stale leader read", stale_leader_read);
      ("add nodes", add_nodes);
      ("bad changes", bad_changes);
      ("remove follower", remove_follower);
      ("remove leader", remove_leader);
      ("remove and add back", remove_and_add_back);
      ("same seed same run", same_seed_same_run);
      ("randomized faults", chaos);
    ]
  in
  let failed = ref 0 in
  List.iter
    (fun (label, f) ->
      match f () with
      | () -> Printf.printf "ok    %s\n%!" label
      | exception e ->
        incr failed;
        Printf.printf "FAIL  %s\n%s\n%!" label (Printexc.to_string e))
    tests;
  if !failed > 0 then exit 1
