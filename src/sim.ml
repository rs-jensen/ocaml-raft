module Node = Raft.Make (Kv)
module IntMap = Map.Make (Int)

type params = {
  size : int;
  seed : int;
  steps : int;
  drop : float;
  delay : int;
  faults : float;
  load : float;
  reads : float;
  spare : int;
  changes : bool;
  compact_after : int;
  batch : int;
  pre_vote : bool;
  check_quorum : bool;
}

let defaults =
  {
    size = 5;
    seed = 1;
    steps = 10_000;
    drop = 0.02;
    delay = 3;
    faults = 0.01;
    load = 0.3;
    reads = 0.1;
    spare = 0;
    changes = true;
    compact_after = 64;
    batch = 64;
    pre_vote = true;
    check_quorum = true;
  }

exception Violation of string

let fail fmt = Printf.ksprintf (fun s -> raise (Violation s)) fmt

module Net = Map.Make (struct
  type t = int * int

  let compare (a, b) (c, d) = if a <> c then Int.compare a c else Int.compare b d
end)

type packet = { src : int; dst : int; msg : Node.message }

type stats = {
  mutable delivered : int;
  mutable lost : int;
  mutable snapshots : int;
  mutable proposed : int;
  mutable confirmed : int;
  mutable elections : int;
  mutable reads_sent : int;
  mutable reads_done : int;
  mutable changes : int;
}

type t = {
  params : params;
  rng : Random.State.t;
  nodes : Node.t array;
  up : bool array;
  retired : bool array;
  side : int array;
  checked : int array;
  mutable net : packet Net.t;
  mutable seq : int;
  mutable now : int;
  mutable lossy : bool;
  mutable next_cmd : int;
  mutable acked_upto : int;
  mutable history : Kv.t IntMap.t;
  leaders : (int, int) Hashtbl.t;
  committed : (int, int) Hashtbl.t;
  applied : (int, int) Hashtbl.t;
  placed : (int, int) Hashtbl.t;
  waiting : (int, int) Hashtbl.t;
  pending_reads : (int * int, int) Hashtbl.t;
  stats : stats;
  trace : int -> string -> unit;
}

let create ?(trace = fun _ _ -> ()) p =
  if p.size < 1 then invalid_arg "Sim.create: empty cluster";
  if p.spare < 0 || p.spare >= p.size then invalid_arg "Sim.create: bad number of spare nodes";
  let start = p.size - p.spare in
  let members = List.init start (fun i -> i) in
  let node id =
    let members = if id < start then members else [] in
    Raft.config ~members ~compact_after:p.compact_after ~max_batch:p.batch
      ~pre_vote:p.pre_vote ~check_quorum:p.check_quorum
      ~seed:(Hashtbl.hash (p.seed, id))
      id
    |> Node.create
  in
  {
    params = { p with delay = max 1 p.delay };
    rng = Random.State.make [| p.seed |];
    nodes = Array.init p.size node;
    up = Array.init p.size (fun id -> id < start);
    retired = Array.init p.size (fun id -> id >= start);
    side = Array.make p.size 0;
    checked = Array.make p.size 0;
    net = Net.empty;
    seq = 0;
    now = 0;
    lossy = true;
    next_cmd = 0;
    acked_upto = 0;
    history = IntMap.empty;
    leaders = Hashtbl.create 64;
    committed = Hashtbl.create 1024;
    applied = Hashtbl.create 1024;
    placed = Hashtbl.create 1024;
    waiting = Hashtbl.create 64;
    pending_reads = Hashtbl.create 64;
    stats =
      {
        delivered = 0;
        lost = 0;
        snapshots = 0;
        proposed = 0;
        confirmed = 0;
        elections = 0;
        reads_sent = 0;
        reads_done = 0;
        changes = 0;
      };
    trace;
  }

let say s fmt = Printf.ksprintf (s.trace s.now) fmt
let ids s = List.init (Array.length s.nodes) (fun i -> i)
let pick s l = List.nth l (Random.State.int s.rng (List.length l))
let chance s p = Random.State.float s.rng 1.0 < p
let name id = "n" ^ string_of_int id

let state_upto s index =
  match IntMap.find_last_opt (fun k -> k <= index) s.history with
  | Some (_, st) -> st
  | None -> Kv.empty

let on_apply s id index (c : Kv.command) result =
  (match Hashtbl.find_opt s.applied index with
   | Some other when other <> c.id ->
     fail "%s applied command %d at index %d where others applied %d" (name id) c.id index other
   | Some _ -> ()
   | None ->
     Hashtbl.replace s.applied index c.id;
     let after, _ = Kv.apply (state_upto s (index - 1)) c in
     s.history <- IntMap.add index after s.history);
  let _, expected = Kv.apply (state_upto s (index - 1)) c in
  if expected <> result then fail "%s got a different result for command %d" (name id) c.id;
  (match Hashtbl.find_opt s.placed c.id with
   | Some i when i <> index -> fail "command %d applied at both %d and %d" c.id i index
   | Some _ -> ()
   | None -> Hashtbl.replace s.placed c.id index);
  match Hashtbl.find_opt s.waiting c.id with
  | Some origin when origin = id ->
    Hashtbl.remove s.waiting c.id;
    s.stats.confirmed <- s.stats.confirmed + 1;
    s.acked_upto <- max s.acked_upto index
  | _ -> ()

let on_read s id rid index state =
  match Hashtbl.find_opt s.pending_reads (id, rid) with
  | None -> fail "%s answered read %d that nobody asked for" (name id) rid
  | Some mark ->
    Hashtbl.remove s.pending_reads (id, rid);
    if index < mark then
      fail "%s answered a read at index %d but index %d was already acknowledged" (name id) index
        mark;
    if not (Kv.equal state (state_upto s index)) then
      fail "%s answered a read with the wrong state for index %d" (name id) index;
    s.stats.reads_done <- s.stats.reads_done + 1

let dispatch s src outputs =
  let handle o =
    match o with
    | Node.Send (dst, msg) ->
      if s.lossy && chance s s.params.drop then s.stats.lost <- s.stats.lost + 1
      else begin
        let at = s.now + 1 + Random.State.int s.rng s.params.delay in
        s.seq <- s.seq + 1;
        s.net <- Net.add (at, s.seq) { src; dst; msg } s.net
      end
    | Node.Apply a -> on_apply s src a.index a.command a.result
    | Node.Read r -> on_read s src r.id r.index r.state
  in
  List.iter handle outputs

let rec deliver s =
  match Net.min_binding_opt s.net with
  | Some (((at, _) as key), pkt) when at <= s.now ->
    s.net <- Net.remove key s.net;
    if s.up.(pkt.dst) && s.side.(pkt.src) = s.side.(pkt.dst) then begin
      s.stats.delivered <- s.stats.delivered + 1;
      (match pkt.msg with
       | Node.Snapshot _ -> s.stats.snapshots <- s.stats.snapshots + 1
       | _ -> ());
      let node, out = Node.receive s.nodes.(pkt.dst) ~from:pkt.src pkt.msg in
      s.nodes.(pkt.dst) <- node;
      dispatch s pkt.dst out
    end
    else s.stats.lost <- s.stats.lost + 1;
    deliver s
  | _ -> ()

let leader s =
  let best = ref None in
  for id = 0 to Array.length s.nodes - 1 do
    let node = s.nodes.(id) in
    if s.up.(id) && Node.role node = Node.Leader then begin
      match !best with
      | Some (_, t) when t >= Node.term node -> ()
      | _ -> best := Some (id, Node.term node)
    end
  done;
  match !best with
  | Some (id, _) -> Some id
  | None -> None

let members s =
  match leader s with
  | Some l -> Node.members s.nodes.(l)
  | None ->
    let best = ref s.nodes.(0) in
    Array.iter (fun n -> if Node.last_index n > Node.last_index !best then best := n) s.nodes;
    Node.members !best

let crash s id =
  if s.up.(id) then begin
    s.up.(id) <- false;
    say s "%s crashed" (name id)
  end

let restart s id =
  if not s.up.(id) then begin
    s.nodes.(id) <- Node.restart s.nodes.(id);
    s.up.(id) <- true;
    say s "%s restarted at term %d" (name id) (Node.term s.nodes.(id))
  end

let partition s groups =
  Array.fill s.side 0 (Array.length s.side) 0;
  List.iteri (fun g group -> List.iter (fun id -> s.side.(id) <- g + 1) group) groups;
  let show group = String.concat "," (List.map name group) in
  say s "partition %s" (String.concat " | " (List.map show groups))

let heal s =
  if Array.exists (fun g -> g <> 0) s.side then begin
    Array.fill s.side 0 (Array.length s.side) 0;
    say s "network healed"
  end

let hand_over s from target =
  let node, out = Node.transfer s.nodes.(from) target in
  s.nodes.(from) <- node;
  dispatch s from out;
  say s "%s hands leadership to %s" (name from) (name target)

let add_node s ~via id =
  match Node.add_member s.nodes.(via) id with
  | Ok (_, node, out) ->
    s.nodes.(via) <- node;
    s.stats.changes <- s.stats.changes + 1;
    if s.retired.(id) then begin
      s.retired.(id) <- false;
      restart s id
    end;
    say s "%s adds %s" (name via) (name id);
    dispatch s via out;
    true
  | Error _ -> false

let remove_node s ~via id =
  match Node.remove_member s.nodes.(via) id with
  | Ok (_, node, out) ->
    s.nodes.(via) <- node;
    s.stats.changes <- s.stats.changes + 1;
    say s "%s removes %s" (name via) (name id);
    dispatch s via out;
    true
  | Error _ -> false

let retire_removed s =
  let newest = ref s.nodes.(0) in
  Array.iter (fun n -> if Node.commit_index n > Node.commit_index !newest then newest := n) s.nodes;
  let committed = Node.committed_members !newest in
  let wanted id =
    let found = ref (List.mem id committed) in
    Array.iteri
      (fun j n -> if j <> id && s.up.(j) && List.mem id (Node.members n) then found := true)
      s.nodes;
    !found
  in
  List.iter
    (fun id ->
      let inside = wanted id in
      if (not inside) && not s.retired.(id) then begin
        s.retired.(id) <- true;
        crash s id;
        say s "%s is out of the cluster" (name id)
      end
      else if inside && s.retired.(id) then begin
        s.retired.(id) <- false;
        restart s id
      end)
    (ids s)

let random_command s =
  let key = String.make 1 (Char.chr (97 + Random.State.int s.rng 8)) in
  let op =
    match Random.State.int s.rng 4 with
    | 0 -> Kv.Put (Random.State.int s.rng 100)
    | 1 -> Kv.Del
    | _ -> Kv.Add (1 + Random.State.int s.rng 9)
  in
  { Kv.id = s.next_cmd; key; op }

let submit ?at s =
  let command = random_command s in
  let rec go id hops =
    match Node.propose s.nodes.(id) command with
    | Ok (_, node, out) ->
      s.nodes.(id) <- node;
      s.next_cmd <- s.next_cmd + 1;
      s.stats.proposed <- s.stats.proposed + 1;
      Hashtbl.replace s.waiting command.id id;
      dispatch s id out;
      true
    | Error (Some l) when hops > 0 && s.up.(l) -> go l (hops - 1)
    | Error _ -> false
  in
  let up = List.filter (fun id -> s.up.(id)) (ids s) in
  match at with
  | Some id -> s.up.(id) && go id 0
  | None -> up <> [] && go (pick s up) 1

let read ?at s =
  let rec go id hops =
    match Node.read s.nodes.(id) with
    | Ok (rid, node, out) ->
      s.nodes.(id) <- node;
      s.stats.reads_sent <- s.stats.reads_sent + 1;
      Hashtbl.replace s.pending_reads (id, rid) s.acked_upto;
      dispatch s id out;
      true
    | Error (Some l) when hops > 0 && s.up.(l) -> go l (hops - 1)
    | Error _ -> false
  in
  let up = List.filter (fun id -> s.up.(id)) (ids s) in
  match at with
  | Some id -> s.up.(id) && go id 0
  | None -> up <> [] && go (pick s up) 1

let fault s =
  let ms = members s in
  let up = List.filter (fun id -> s.up.(id)) (ids s) in
  let down = List.filter (fun id -> (not s.up.(id)) && not s.retired.(id)) (ids s) in
  let down_members = List.filter (fun id -> List.mem id ms) down in
  let room = List.length down_members < (List.length ms - 1) / 2 in
  match (Random.State.int s.rng 10, leader s) with
  | 0, _ when room && up <> [] -> crash s (pick s up)
  | 1, Some l when room -> crash s l
  | (2 | 3), _ when down <> [] -> restart s (pick s down)
  | 4, _ ->
    let a, b = List.partition (fun _ -> Random.State.bool s.rng) (ids s) in
    if a = [] || b = [] then heal s else partition s [ a; b ]
  | 5, Some l -> partition s [ [ l ] ]
  | 6, _ -> heal s
  | 7, Some l -> (
    match List.filter (fun id -> id <> l) (Node.members s.nodes.(l)) with
    | [] -> ()
    | targets -> hand_over s l (pick s targets))
  | 8, Some l when s.params.changes -> (
    match List.filter (fun id -> not (List.mem id (Node.members s.nodes.(l)))) (ids s) with
    | [] -> ()
    | outside -> ignore (add_node s ~via:l (pick s outside)))
  | 9, Some l when s.params.changes ->
    let ms = Node.members s.nodes.(l) in
    if List.length ms > 2 then ignore (remove_node s ~via:l (pick s ms))
  | _ -> ()

let check_leaders s =
  Array.iteri
    (fun id node ->
      if s.up.(id) && Node.role node = Node.Leader then begin
        let term = Node.term node in
        match Hashtbl.find_opt s.leaders term with
        | Some other when other <> id ->
          fail "term %d has two leaders, %s and %s" term (name other) (name id)
        | Some _ -> ()
        | None ->
          Hashtbl.replace s.leaders term id;
          s.stats.elections <- s.stats.elections + 1;
          say s "%s leads term %d" (name id) term;
          Hashtbl.iter
            (fun index t ->
              if index > Node.snapshot_index node && Node.entry_term node index <> Some t then
                fail "%s leads term %d without committed entry %d" (name id) term index)
            s.committed
      end)
    s.nodes

let check_commits ?(full = false) s =
  Array.iteri
    (fun id node ->
      if s.up.(id) then begin
        let from = if full then Node.first_index node else s.checked.(id) + 1 in
        let from = max from (Node.first_index node) in
        for index = from to Node.commit_index node do
          match (Node.entry_term node index, Hashtbl.find_opt s.committed index) with
          | None, _ -> fail "%s lost committed entry %d" (name id) index
          | Some t, Some t2 when t <> t2 ->
            fail "index %d committed in term %d on %s but in term %d elsewhere" index t (name id)
              t2
          | Some _, Some _ -> ()
          | Some t, None -> Hashtbl.replace s.committed index t
        done;
        s.checked.(id) <- max s.checked.(id) (Node.commit_index node)
      end)
    s.nodes

let check_matching s =
  let n = Array.length s.nodes in
  for i = 0 to n - 1 do
    for j = i + 1 to n - 1 do
      let a = s.nodes.(i) and b = s.nodes.(j) in
      let lo = max (Node.first_index a) (Node.first_index b) in
      let hi = min (Node.last_index a) (Node.last_index b) in
      let agreed = ref false in
      for k = hi downto lo do
        match (Node.entry a k, Node.entry b k) with
        | Some x, Some y when x.term = y.term ->
          if x <> y then fail "%s and %s hold different entries at %d" (name i) (name j) k;
          agreed := true
        | _ ->
          if !agreed then fail "%s and %s agree after index %d but not at it" (name i) (name j) k
      done
    done
  done

let step ?(faults = true) ?(load = true) s =
  s.now <- s.now + 1;
  if faults && chance s s.params.faults then fault s;
  for id = 0 to Array.length s.nodes - 1 do
    if s.up.(id) then begin
      let node, out = Node.tick s.nodes.(id) in
      s.nodes.(id) <- node;
      dispatch s id out
    end
  done;
  deliver s;
  if load && chance s s.params.load then ignore (submit s);
  if load && chance s s.params.reads then ignore (read s);
  retire_removed s;
  check_leaders s;
  check_commits ~full:(s.now mod 50 = 0) s;
  if s.now mod 50 = 0 then check_matching s

let converged s =
  match leader s with
  | None -> false
  | Some l ->
    let top = Node.last_index s.nodes.(l) and term = Node.term s.nodes.(l) in
    Array.for_all (fun x -> x) s.up
    && Array.for_all
         (fun node ->
           Node.term node = term
           && Node.leader node = Some l
           && Node.last_index node = top
           && Node.applied_index node = top)
         s.nodes

let everyone_in s =
  match leader s with
  | None -> false
  | Some l -> (
    let ms = Node.members s.nodes.(l) in
    match List.filter (fun id -> not (List.mem id ms)) (ids s) with
    | [] -> not (Node.change_pending s.nodes.(l))
    | id :: _ ->
      ignore (add_node s ~via:l id);
      false)

let settle ?(within = 4000) s =
  heal s;
  List.iter (fun id -> if not s.retired.(id) then restart s id) (ids s);
  s.lossy <- false;
  let deadline = s.now + within in
  let rec until f =
    if not (f ()) then begin
      if s.now >= deadline then fail "cluster still unsettled %d ticks after healing" within;
      step ~faults:false ~load:false s;
      until f
    end
  in
  let ready () = everyone_in s && converged s in
  let rec confirm () =
    until ready;
    let marker = s.next_cmd in
    until (fun () -> submit s);
    until ready;
    if not (Hashtbl.mem s.placed marker) then confirm ()
  in
  confirm ();
  let reads = s.stats.reads_done in
  until (fun () -> read s);
  until (fun () -> s.stats.reads_done > reads);
  check_commits ~full:true s;
  check_matching s;
  let reference = Node.state s.nodes.(0) in
  Array.iteri
    (fun id node ->
      if not (Kv.equal reference (Node.state node)) then
        fail "%s ended with a different state than %s" (name id) (name 0))
    s.nodes

let run ?trace p =
  let s = create ?trace p in
  for _ = 1 to p.steps do
    step s
  done;
  settle s;
  s
