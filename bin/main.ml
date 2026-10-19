open Sim

let summary s =
  let p = s.params and st = s.stats in
  Printf.printf "%d nodes, seed %d, %d ticks\n\n" p.size p.seed s.now;
  Printf.printf "elections   %d\n" st.elections;
  Printf.printf "writes      %d accepted, %d confirmed\n" st.proposed st.confirmed;
  Printf.printf "reads       %d asked, %d answered\n" st.reads_sent st.reads_done;
  Printf.printf "changes     %d membership changes\n" st.changes;
  Printf.printf "messages    %d delivered, %d lost, %d snapshots\n\n" st.delivered st.lost
    st.snapshots;
  Printf.printf "node  role          term   commit  snapshot\n";
  Array.iteri
    (fun id n ->
      Printf.printf "n%-4d %-12s %5d %8d %9d\n" id
        (Node.role_name (Node.role n))
        (Node.term n) (Node.commit_index n) (Node.snapshot_index n))
    s.nodes;
  let pairs = List.map (fun (k, v) -> k ^ "=" ^ string_of_int v) (Kv.to_list (Node.state s.nodes.(0))) in
  let state = String.concat " " pairs in
  Printf.printf "\nstate       %s\n" (if state = "" then "(empty)" else state)

let () =
  let p = ref defaults in
  let verbose = ref false in
  let runs = ref 1 in
  let int f = Arg.Int (fun v -> p := f !p v) in
  let float f = Arg.Float (fun v -> p := f !p v) in
  let off f = Arg.Unit (fun () -> p := f !p) in
  let spec =
    [
      ("-nodes", int (fun p size -> { p with size }), "N cluster size (5)");
      ("-spare", int (fun p spare -> { p with spare }), "N nodes that start outside the cluster (0)");
      ("-seed", int (fun p seed -> { p with seed }), "N random seed (1)");
      ("-steps", int (fun p steps -> { p with steps }), "N ticks before healing (10000)");
      ("-drop", float (fun p drop -> { p with drop }), "P message loss (0.02)");
      ("-delay", int (fun p delay -> { p with delay }), "N max network delay in ticks (3)");
      ("-faults", float (fun p faults -> { p with faults }), "P fault chance per tick (0.01)");
      ("-load", float (fun p load -> { p with load }), "P write chance per tick (0.3)");
      ("-reads", float (fun p reads -> { p with reads }), "P read chance per tick (0.1)");
      ("-compact", int (fun p n -> { p with compact_after = n }), "N snapshot interval (64)");
      ("-batch", int (fun p batch -> { p with batch }), "N max entries per append (64)");
      ("-no-changes", off (fun p -> { p with changes = false }), " no membership changes");
      ("-no-prevote", off (fun p -> { p with pre_vote = false }), " disable pre-vote");
      ("-no-checkquorum", off (fun p -> { p with check_quorum = false }), " disable check quorum");
      ("-runs", Arg.Set_int runs, "N run N seeds in a row and only print failures (1)");
      ("-v", Arg.Set verbose, " print cluster events");
    ]
  in
  Arg.parse (Arg.align spec) (fun a -> raise (Arg.Bad a)) "main.exe [options]";
  let trace now msg = if !verbose then Printf.printf "%7d  %s\n%!" now msg in
  if !runs <= 1 then begin
    match run ~trace !p with
    | s -> summary s
    | exception Violation msg ->
      Printf.eprintf "seed %d: %s\n" !p.seed msg;
      exit 1
  end
  else begin
    let bad = ref 0 in
    for i = 0 to !runs - 1 do
      let seed = !p.seed + i in
      match run { !p with seed } with
      | _ -> ()
      | exception Violation msg ->
        incr bad;
        Printf.printf "seed %d: %s\n%!" seed msg
    done;
    Printf.printf "%d runs, %d failed\n" !runs !bad;
    if !bad > 0 then exit 1
  end
