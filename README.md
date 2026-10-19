# ocaml-raft

Raft consensus written in OCaml. Only uses the standard library.

`Raft.Make` takes a state machine and gives you a node. A node is just an immutable value.
You call `tick`, `receive`, `propose`, `read` and so on, and you get back the new node plus
a list of things to do (messages to send, entries that got applied, reads that are ready).
Timers, network and disk are up to you.

What it does:

- leader election with random timeouts, pre-vote and check quorum
- log replication with pipelining and fast backtracking on conflicts
- snapshots and InstallSnapshot for nodes that fall behind
- leadership transfer
- linearizable reads (ReadIndex), many reads share one heartbeat round
- adding and removing nodes one at a time, the leader can also remove itself

## Files

    src/raft.ml   the protocol
    src/kv.ml     small key value store used as the state machine
    src/sim.ml    fake cluster with message loss, delays, partitions, crashes and membership changes
    bin/main.ml   runs the simulator from the command line
    test/         tests

The simulator checks the Raft safety rules after every tick: one leader per term, logs match,
leaders have all committed entries, and every node applies the same thing at the same index.
It also checks that reads never return stale data. At the end it heals everything, adds back
any removed nodes and checks that all nodes end up with the same state.

## Running

    dune build
    dune test
    dune exec bin/main.exe -- -nodes 5 -seed 7 -steps 20000 -v
    dune exec bin/main.exe -- -runs 500 -faults 0.05

`-help` shows all the options.
