module IntMap = Map.Make (Int)
module IntSet = Set.Make (Int)

module type MACHINE = sig
  type t
  type command
  type result

  val empty : t
  val apply : t -> command -> t * result
end

type config = {
  id : int;
  members : int list;
  election_ticks : int;
  heartbeat_ticks : int;
  max_batch : int;
  compact_after : int;
  pre_vote : bool;
  check_quorum : bool;
  seed : int;
}

let config ?(election_ticks = 10) ?(heartbeat_ticks = 2) ?(max_batch = 64)
    ?(compact_after = 1024) ?(pre_vote = true) ?(check_quorum = true) ?seed
    ~members id =
  let members = List.sort_uniq compare members in
  if members <> [] && not (List.mem id members) then
    invalid_arg "Raft.config: id is not in members";
  if heartbeat_ticks < 1 || heartbeat_ticks >= election_ticks then
    invalid_arg "Raft.config: need 0 < heartbeat_ticks < election_ticks";
  let seed = match seed with Some s -> s | None -> (id * 7919) + 1 in
  {
    id;
    members;
    election_ticks;
    heartbeat_ticks;
    max_batch = max 1 max_batch;
    compact_after = max 1 compact_after;
    pre_vote;
    check_quorum;
    seed;
  }

let next_rand x = ((x * 1103515245) + 12345) land 0x3fffffff

module Make (M : MACHINE) = struct
  type data = Noop | Cmd of M.command | Members of int list
  type entry = { term : int; index : int; data : data }

  type snapshot = {
    snap_index : int;
    snap_term : int;
    snap_members : int list;
    snap_state : M.t;
  }

  module Log = struct
    type t = {
      snap : snapshot;
      entries : entry IntMap.t;
      confs : int list IntMap.t;
      last : int;
    }

    let create snap =
      { snap; entries = IntMap.empty; confs = IntMap.empty; last = snap.snap_index }

    let base l = l.snap.snap_index
    let last l = l.last
    let get l i = IntMap.find_opt i l.entries

    let term l i =
      if i = base l then Some l.snap.snap_term
      else
        match get l i with
        | Some e -> Some e.term
        | None -> None

    let last_term l =
      match term l l.last with
      | Some t -> t
      | None -> l.snap.snap_term

    let members l =
      match IntMap.max_binding_opt l.confs with
      | Some (_, m) -> m
      | None -> l.snap.snap_members

    let members_at l i =
      match IntMap.find_last_opt (fun k -> k <= i) l.confs with
      | Some (_, m) -> m
      | None -> l.snap.snap_members

    let last_change l =
      match IntMap.max_binding_opt l.confs with
      | Some (i, _) -> i
      | None -> base l

    let add l e =
      let confs =
        match e.data with
        | Members m -> IntMap.add e.index m l.confs
        | _ -> l.confs
      in
      { l with entries = IntMap.add e.index e l.entries; confs; last = e.index }

    let cut l i =
      let entries, _, _ = IntMap.split i l.entries in
      let confs, _, _ = IntMap.split i l.confs in
      { l with entries; confs; last = i - 1 }

    let range l lo hi =
      let rec loop i acc =
        if i < lo then acc
        else
          match get l i with
          | Some e -> loop (i - 1) (e :: acc)
          | None -> loop (i - 1) acc
      in
      loop hi []

    let rec merge l es =
      match es with
      | [] -> l
      | e :: rest ->
        if e.index <= base l then merge l rest
        else begin
          match term l e.index with
          | Some t when t = e.term -> merge l rest
          | Some _ -> List.fold_left add (cut l e.index) es
          | None -> List.fold_left add l es
        end

    let compact l snap =
      let _, _, entries = IntMap.split snap.snap_index l.entries in
      let _, _, confs = IntMap.split snap.snap_index l.confs in
      { snap; entries; confs; last = max l.last snap.snap_index }

    let install l snap =
      if term l snap.snap_index = Some snap.snap_term then compact l snap
      else create snap

    let first_of_term l i =
      let t = term l i in
      let i = ref i in
      while !i - 1 > base l && term l (!i - 1) = t do
        decr i
      done;
      !i

    let last_of_term l t =
      let rec loop i =
        match term l i with
        | Some x when x = t -> Some i
        | Some x when x > t && i > base l -> loop (i - 1)
        | _ -> None
      in
      loop l.last

    let up_to_date l index t =
      let mine = last_term l in
      t > mine || (t = mine && index >= l.last)
  end

  type message =
    | Vote_req of {
        term : int;
        last_index : int;
        last_term : int;
        pre : bool;
        forced : bool;
      }
    | Vote_resp of { term : int; granted : bool; pre : bool }
    | Append of {
        term : int;
        prev_index : int;
        prev_term : int;
        entries : entry list;
        commit : int;
        round : int;
      }
    | Append_resp of {
        term : int;
        ok : bool;
        index : int;
        conflict : int option;
        probe : int;
        round : int;
      }
    | Snapshot of { term : int; snap : snapshot; round : int }
    | Snapshot_resp of { term : int; index : int; round : int }
    | Timeout_now of { term : int }

  type output =
    | Send of int * message
    | Apply of { index : int; term : int; command : M.command; result : M.result }
    | Read of { id : int; index : int; state : M.t }

  type role = Follower | Pre_candidate | Candidate | Leader

  type peer = {
    next : int;
    matched : int;
    sent : int;
    probing : bool;
    active : bool;
    acked : int;
  }

  type read = { read_id : int; read_index : int; read_round : int }

  type t = {
    cfg : config;
    cur_term : int;
    voted_for : int option;
    log : Log.t;
    role : role;
    leader : int option;
    commit : int;
    applied : int;
    state : M.t;
    votes : IntSet.t;
    rejects : IntSet.t;
    peers : peer IntMap.t;
    elapsed : int;
    timeout : int;
    beat : int;
    rand : int;
    transfer_to : int option;
    round : int;
    reads : read list;
    read_count : int;
    want_beat : bool;
    term_start : int;
    out : output list;
  }

  let msg_term m =
    match m with
    | Vote_req r -> r.term
    | Vote_resp r -> r.term
    | Append r -> r.term
    | Append_resp r -> r.term
    | Snapshot r -> r.term
    | Snapshot_resp r -> r.term
    | Timeout_now r -> r.term

  let members n = Log.members n.log
  let is_member n id = List.mem id (members n)
  let quorum n = (List.length (members n) / 2) + 1
  let others n = List.filter (fun id -> id <> n.cfg.id) (members n)
  let emit o n = { n with out = o :: n.out }
  let send dst m n = emit (Send (dst, m)) n

  let new_timeout n =
    let rand = next_rand n.rand in
    let e = n.cfg.election_ticks in
    { n with rand; elapsed = 0; timeout = e + ((rand lsr 8) mod e) }

  let in_lease n =
    n.cfg.check_quorum && n.leader <> None && n.elapsed < n.cfg.election_ticks

  let create cfg =
    let snap =
      { snap_index = 0; snap_term = 0; snap_members = cfg.members; snap_state = M.empty }
    in
    new_timeout
      {
        cfg;
        cur_term = 0;
        voted_for = None;
        log = Log.create snap;
        role = Follower;
        leader = None;
        commit = 0;
        applied = 0;
        state = M.empty;
        votes = IntSet.empty;
        rejects = IntSet.empty;
        peers = IntMap.empty;
        elapsed = 0;
        timeout = 0;
        beat = 0;
        rand = cfg.seed;
        transfer_to = None;
        round = 0;
        reads = [];
        read_count = 0;
        want_beat = false;
        term_start = 0;
        out = [];
      }

  let become_follower ?leader term n =
    let n = if term > n.cur_term then { n with cur_term = term; voted_for = None } else n in
    new_timeout
      {
        n with
        role = Follower;
        leader;
        votes = IntSet.empty;
        rejects = IntSet.empty;
        peers = IntMap.empty;
        transfer_to = None;
        reads = [];
        want_beat = false;
      }

  let compact n =
    let size = n.applied - Log.base n.log in
    let behind = IntMap.exists (fun _ p -> p.active && p.matched < n.applied) n.peers in
    if size < n.cfg.compact_after then n
    else if behind && size < 4 * n.cfg.compact_after then n
    else
      match Log.term n.log n.applied with
      | None -> n
      | Some t ->
        let snap =
          {
            snap_index = n.applied;
            snap_term = t;
            snap_members = Log.members_at n.log n.applied;
            snap_state = n.state;
          }
        in
        { n with log = Log.compact n.log snap }

  let rec apply_committed n =
    if n.applied >= n.commit then compact n
    else
      let i = n.applied + 1 in
      match Log.get n.log i with
      | Some { data = Cmd c; term; _ } ->
        let state, result = M.apply n.state c in
        let n = { n with state; applied = i } in
        apply_committed (emit (Apply { index = i; term; command = c; result }) n)
      | Some _ -> apply_committed { n with applied = i }
      | None -> failwith "raft: committed entry is missing"

  let replicate ?(force = false) id n =
    match IntMap.find_opt id n.peers with
    | None -> n
    | Some p ->
      if p.next <= Log.base n.log then begin
        if force then
          send id (Snapshot { term = n.cur_term; snap = n.log.Log.snap; round = n.round }) n
        else n
      end
      else if p.probing && not force then n
      else begin
        let from = if force then p.next else p.sent + 1 in
        let last = Log.last n.log in
        if from > last && not force then n
        else begin
          let upto = min last (from + n.cfg.max_batch - 1) in
          let prev_term = match Log.term n.log (from - 1) with Some t -> t | None -> 0 in
          let m =
            Append
              {
                term = n.cur_term;
                prev_index = from - 1;
                prev_term;
                entries = Log.range n.log from upto;
                commit = n.commit;
                round = n.round;
              }
          in
          let p = { p with sent = max p.sent upto } in
          send id m { n with peers = IntMap.add id p n.peers }
        end
      end

  let broadcast ?force n = IntMap.fold (fun id _ n -> replicate ?force id n) n.peers n

  let heartbeat n =
    broadcast ~force:true { n with round = n.round + 1; beat = 0; want_beat = false }

  let sync_peers n =
    let last = Log.last n.log in
    let fresh =
      { next = last + 1; matched = 0; sent = last; probing = true; active = true; acked = 0 }
    in
    let add acc id =
      match IntMap.find_opt id n.peers with
      | Some p -> IntMap.add id p acc
      | None -> IntMap.add id fresh acc
    in
    let leaving =
      if Log.last_change n.log > n.commit then Log.members_at n.log n.commit else []
    in
    let ids = List.sort_uniq compare (members n @ leaving) in
    let ids = List.filter (fun id -> id <> n.cfg.id) ids in
    { n with peers = List.fold_left add IntMap.empty ids }

  let confirmed n round =
    let ok id p = p.acked >= round && is_member n id in
    let count = IntMap.fold (fun id p c -> if ok id p then c + 1 else c) n.peers 0 in
    let count = if is_member n n.cfg.id then count + 1 else count in
    count >= quorum n

  let serve_reads n =
    if n.role <> Leader || n.reads = [] then n
    else begin
      let ready, waiting =
        List.partition (fun r -> n.applied >= r.read_index && confirmed n r.read_round) n.reads
      in
      let serve n r = emit (Read { id = r.read_id; index = n.applied; state = n.state }) n in
      List.fold_left serve { n with reads = waiting } ready
    end

  let leave_if_removed n =
    if n.role = Leader && (not (is_member n n.cfg.id)) && n.commit >= Log.last_change n.log
    then begin
      let last = Log.last n.log in
      let best =
        IntMap.fold
          (fun id p acc -> if p.matched = last && is_member n id then Some id else acc)
          n.peers None
      in
      let n =
        match best with
        | Some id -> send id (Timeout_now { term = n.cur_term }) n
        | None -> n
      in
      become_follower n.cur_term n
    end
    else n

  let advance n =
    let ms = members n in
    if ms = [] then n
    else begin
      let matched_of id =
        if id = n.cfg.id then Log.last n.log
        else
          match IntMap.find_opt id n.peers with
          | Some p -> p.matched
          | None -> 0
      in
      let sorted = List.sort (fun a b -> compare b a) (List.map matched_of ms) in
      let q = List.nth sorted (quorum n - 1) in
      if q > n.commit && Log.term n.log q = Some n.cur_term then
        leave_if_removed (sync_peers (apply_committed { n with commit = q }))
      else n
    end

  let become_leader n =
    let start = Log.last n.log + 1 in
    let n =
      {
        n with
        role = Leader;
        leader = Some n.cfg.id;
        peers = IntMap.empty;
        beat = 0;
        elapsed = 0;
        term_start = start;
        reads = [];
      }
    in
    let n = sync_peers n in
    let n = { n with log = Log.add n.log { term = n.cur_term; index = start; data = Noop } } in
    heartbeat (advance n)

  let rec campaign ?(forced = false) pre n =
    let n =
      if pre then { n with role = Pre_candidate; leader = None }
      else
        {
          n with
          role = Candidate;
          leader = None;
          cur_term = n.cur_term + 1;
          voted_for = Some n.cfg.id;
        }
    in
    let n = new_timeout { n with votes = IntSet.singleton n.cfg.id; rejects = IntSet.empty } in
    let term = if pre then n.cur_term + 1 else n.cur_term in
    let m =
      Vote_req
        { term; last_index = Log.last n.log; last_term = Log.last_term n.log; pre; forced }
    in
    count_votes (List.fold_left (fun n id -> send id m n) n (others n))

  and count_votes n =
    let count s = IntSet.cardinal (IntSet.filter (is_member n) s) in
    let won = count n.votes >= quorum n in
    let lost = count n.rejects >= quorum n in
    match n.role with
    | Pre_candidate when won -> campaign false n
    | Candidate when won -> become_leader n
    | (Pre_candidate | Candidate) when lost -> become_follower n.cur_term n
    | _ -> n

  let check_quorum n =
    let alive =
      IntMap.fold (fun id p c -> if p.active && is_member n id then c + 1 else c) n.peers 0
    in
    let alive = if is_member n n.cfg.id then alive + 1 else alive in
    let peers = IntMap.map (fun p -> { p with active = false }) n.peers in
    let n = { n with elapsed = 0; transfer_to = None; peers } in
    if n.cfg.check_quorum && alive < quorum n then become_follower n.cur_term n else n

  let on_tick n =
    if n.role = Leader then begin
      let n = { n with elapsed = n.elapsed + 1; beat = n.beat + 1 } in
      let n = if n.elapsed >= n.cfg.election_ticks then check_quorum n else n in
      if n.role = Leader && (n.want_beat || n.beat >= n.cfg.heartbeat_ticks) then heartbeat n
      else n
    end
    else begin
      let n = { n with elapsed = n.elapsed + 1 } in
      if n.elapsed < n.timeout then n
      else if is_member n n.cfg.id then campaign n.cfg.pre_vote n
      else new_timeout n
    end

  let on_vote_req from term last_index last_term pre n =
    let fresh = Log.up_to_date n.log last_index last_term in
    let free =
      match n.voted_for with
      | Some v -> v = from
      | None -> n.leader = None
    in
    let ok = fresh && (free || (pre && term > n.cur_term)) in
    if pre then
      let t = if ok then term else n.cur_term in
      send from (Vote_resp { term = t; granted = ok; pre = true }) n
    else if ok then begin
      let n = { (become_follower n.cur_term n) with voted_for = Some from } in
      send from (Vote_resp { term = n.cur_term; granted = true; pre = false }) n
    end
    else send from (Vote_resp { term = n.cur_term; granted = false; pre = false }) n

  let on_vote_resp from granted pre n =
    let counts =
      match n.role with
      | Pre_candidate -> pre
      | Candidate -> not pre
      | Follower | Leader -> false
    in
    if not counts then n
    else if granted then count_votes { n with votes = IntSet.add from n.votes }
    else count_votes { n with rejects = IntSet.add from n.rejects }

  let on_append from prev_index prev_term entries leader_commit round n =
    let n = become_follower ~leader:from n.cur_term n in
    let reply ok index conflict n =
      send from
        (Append_resp { term = n.cur_term; ok; index; conflict; probe = prev_index; round })
        n
    in
    let prev, pterm, entries =
      if prev_index >= n.commit then (prev_index, prev_term, entries)
      else begin
        let t = match Log.term n.log n.commit with Some t -> t | None -> 0 in
        (n.commit, t, List.filter (fun e -> e.index > n.commit) entries)
      end
    in
    match Log.term n.log prev with
    | Some t when t = pterm ->
      let last = prev + List.length entries in
      let commit = max n.commit (min leader_commit last) in
      let n = apply_committed { n with log = Log.merge n.log entries; commit } in
      reply true last None n
    | Some t -> reply false (Log.first_of_term n.log prev) (Some t) n
    | None -> reply false (Log.last n.log + 1) None n

  let on_append_resp from ok index conflict probe round n =
    match (n.role, IntMap.find_opt from n.peers) with
    | Leader, Some p ->
      let p = { p with active = true; acked = max p.acked round } in
      if ok then begin
        let matched = max p.matched index in
        let p =
          {
            p with
            matched;
            next = max p.next (matched + 1);
            sent = max p.sent matched;
            probing = false;
          }
        in
        let n = advance { n with peers = IntMap.add from p n.peers } in
        let n = serve_reads (replicate from n) in
        if n.role = Leader && n.transfer_to = Some from && matched = Log.last n.log then
          send from (Timeout_now { term = n.cur_term }) n
        else n
      end
      else if probe < p.matched || (p.probing && probe <> p.next - 1) then
        serve_reads { n with peers = IntMap.add from p n.peers }
      else begin
        let hint =
          match conflict with
          | Some t -> (
            match Log.last_of_term n.log t with
            | Some i -> i + 1
            | None -> index)
          | None -> index
        in
        let next = max (p.matched + 1) (min hint (Log.last n.log + 1)) in
        let p = { p with next; sent = next - 1; probing = true } in
        serve_reads (replicate ~force:true from { n with peers = IntMap.add from p n.peers })
      end
    | _ -> n

  let on_snapshot from snap round n =
    let n = become_follower ~leader:from n.cur_term n in
    if snap.snap_index <= n.commit then
      send from (Snapshot_resp { term = n.cur_term; index = n.commit; round }) n
    else begin
      let n =
        {
          n with
          log = Log.install n.log snap;
          commit = snap.snap_index;
          applied = snap.snap_index;
          state = snap.snap_state;
        }
      in
      send from (Snapshot_resp { term = n.cur_term; index = snap.snap_index; round }) n
    end

  let on_snapshot_resp from index round n =
    match (n.role, IntMap.find_opt from n.peers) with
    | Leader, Some p ->
      let matched = max p.matched index in
      let p =
        {
          next = matched + 1;
          matched;
          sent = matched;
          probing = false;
          active = true;
          acked = max p.acked round;
        }
      in
      let n = advance { n with peers = IntMap.add from p n.peers } in
      serve_reads (replicate from n)
    | _ -> n

  let start_transfer target n =
    match (n.role, IntMap.find_opt target n.peers) with
    | Leader, Some p ->
      let n = { n with transfer_to = Some target; elapsed = 0 } in
      if p.matched = Log.last n.log then send target (Timeout_now { term = n.cur_term }) n
      else replicate ~force:true target n
    | _ -> n

  let handle from m n =
    match m with
    | Vote_req r -> on_vote_req from r.term r.last_index r.last_term r.pre n
    | Vote_resp r -> on_vote_resp from r.granted r.pre n
    | Append r -> on_append from r.prev_index r.prev_term r.entries r.commit r.round n
    | Append_resp r -> on_append_resp from r.ok r.index r.conflict r.probe r.round n
    | Snapshot r -> on_snapshot from r.snap r.round n
    | Snapshot_resp r -> on_snapshot_resp from r.index r.round n
    | Timeout_now _ ->
      if n.role = Leader || not (is_member n n.cfg.id) then n
      else campaign ~forced:true false n

  let on_message from m n =
    let term = msg_term m in
    if term > n.cur_term then begin
      match m with
      | Vote_req r when (not r.forced) && in_lease n -> n
      | Vote_req r when r.pre -> handle from m n
      | Vote_resp r when r.pre && r.granted -> handle from m n
      | Append _ | Snapshot _ | Timeout_now _ ->
        handle from m (become_follower ~leader:from term n)
      | _ -> handle from m (become_follower term n)
    end
    else if term < n.cur_term then begin
      match m with
      | Append _ | Snapshot _ ->
        send from
          (Append_resp
             { term = n.cur_term; ok = false; index = 0; conflict = None; probe = 0; round = 0 })
          n
      | Vote_req r ->
        send from (Vote_resp { term = n.cur_term; granted = false; pre = r.pre }) n
      | _ -> n
    end
    else handle from m n

  let run f n =
    let n = f { n with out = [] } in
    ({ n with out = [] }, List.rev n.out)

  let tick n = run on_tick n
  let receive n ~from m = run (on_message from m) n
  let transfer n target = run (start_transfer target) n

  let propose n c =
    if n.role = Leader && n.transfer_to = None then begin
      let index = Log.last n.log + 1 in
      let go n =
        let n = { n with log = Log.add n.log { term = n.cur_term; index; data = Cmd c } } in
        broadcast (advance n)
      in
      let n, out = run go n in
      Ok (index, n, out)
    end
    else Error n.leader

  let read n =
    if n.role <> Leader then Error n.leader
    else begin
      let id = n.read_count in
      let r =
        { read_id = id; read_index = max n.commit n.term_start; read_round = n.round + 1 }
      in
      let go n =
        serve_reads { n with reads = n.reads @ [ r ]; read_count = id + 1; want_beat = true }
      in
      let n, out = run go n in
      Ok (id, n, out)
    end

  let change_members n wanted =
    let wanted = List.sort_uniq compare wanted in
    let current = members n in
    let missing a b = List.filter (fun x -> not (List.mem x b)) a in
    let diff = List.length (missing wanted current) + List.length (missing current wanted) in
    if n.role <> Leader then Error "not the leader"
    else if n.transfer_to <> None then Error "leadership transfer in progress"
    else if Log.last_change n.log > n.commit then Error "another change is not committed yet"
    else if n.commit < n.term_start then Error "nothing committed in this term yet"
    else if wanted = [] then Error "cluster can not be empty"
    else if diff <> 1 then Error "can only add or remove one node at a time"
    else begin
      let index = Log.last n.log + 1 in
      let go n =
        let n =
          { n with log = Log.add n.log { term = n.cur_term; index; data = Members wanted } }
        in
        broadcast (advance (sync_peers n))
      in
      let n, out = run go n in
      Ok (index, n, out)
    end

  let add_member n id = change_members n (id :: members n)
  let remove_member n id = change_members n (List.filter (fun x -> x <> id) (members n))

  let restart n =
    let s = n.log.Log.snap in
    new_timeout
      {
        n with
        role = Follower;
        leader = None;
        commit = s.snap_index;
        applied = s.snap_index;
        state = s.snap_state;
        votes = IntSet.empty;
        rejects = IntSet.empty;
        peers = IntMap.empty;
        beat = 0;
        transfer_to = None;
        reads = [];
        want_beat = false;
        out = [];
      }

  let id n = n.cfg.id
  let role n = n.role
  let term n = n.cur_term
  let leader n = n.leader
  let voted_for n = n.voted_for
  let commit_index n = n.commit
  let applied_index n = n.applied
  let state n = n.state
  let change_pending n = Log.last_change n.log > n.commit
  let committed_members n = Log.members_at n.log n.commit
  let snapshot_index n = Log.base n.log
  let first_index n = Log.base n.log + 1
  let last_index n = Log.last n.log
  let entry n i = Log.get n.log i
  let entry_term n i = Log.term n.log i

  let role_name r =
    match r with
    | Follower -> "follower"
    | Pre_candidate -> "precandidate"
    | Candidate -> "candidate"
    | Leader -> "leader"
end
