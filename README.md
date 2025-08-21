# KVStore

## Installation

If [available in Hex](https://hex.pm/docs/publish), the package can be installed
by adding `kv_store` to your list of dependencies in `mix.exs`:

```elixir
def deps do
  [
    {:kv_store, "~> 0.1.0"}
  ]
end
```

Documentation can be generated with [ExDoc](https://github.com/elixir-lang/ex_doc)
and published on [HexDocs](https://hexdocs.pm). Once published, the docs can
be found at <https://hexdocs.pm/kv_store>.


# Problem Statement
We rely on a variety of database management systems and many of these systems have a pluggable architecture.

These architectures include a component called the
storage engine which is responsible for the maintenance of persistent data within the database management system.

A lot of modern database management systems implement these storage engines as persistent Key/Value systems.


The objective of this task is to implement a network-available persistent Key/Value system that exposes the interfaces listed below in any programming language of choice.

You should rely only on the standard libraries of the language.

1. Put(Key, Value)
2. Read(Key)
3. ReadKeyRange(StartKey, EndKey)
4. BatchPut(..keys, ..values)
5. Delete(key)


The implementation should strive to achieve the following requirements

1. Low latency per item read or written
2. High throughput, especially when writing an incoming stream of random items
3. Ability to handle datasets much larger than RAM w/o degradation
4. Crash friendliness, both in terms of fast recovery and not losing data
5. Predictable behavior under heavy access load or large volume


You are at liberty to make any trade off to achieve these objectives.
Bonus points
1. Replicate data to multiple nodes
2. Handle automatic failover to the other nodes

How to Submit
Create a Github repository and commit your work, Within the repository provide documentation on how to run the finished product.

Share a link to the Github repo.

# References

1. https://static.googleusercontent.com/media/research.google.com/en//archive/bigtable-osdi06.pdf
2. https://riak.com/assets/bitcask-intro.pdf
3. https://www.cs.umb.edu/~poneil/lsmtree.pdf
4. https://web.stanford.edu/~ouster/cgi-bin/papers/raft-atc14.pdf
5. https://lamport.azurewebsites.net/pubs/lamport-paxos.pdf
6. https://lamport.azurewebsites.net/pubs/paxos-simple.pdf


# Implemetaion Details that we will take :


# Why elixir ?
I chose elixir for this project because of the fault tolerance, OTP, GenServer, ETS & Node clustering that elixir language provides by default with in the langugage itself.

Also chose the Bitcask paper for implementing the key/vale storage engine as this approach is easier to implement and later if we want to migrate this into a multiple writes or localised application DB that is distributed and localised we can easily implement consensus and avoid write conflicts using CRDT. Inspiration from Riak DB. Not taking the route of Big Table.

### Steps of Implementation :

Awesome problem. Here’s a pragmatic, Bitcask-style plan to build a **network-available, persistent K/V store in Elixir** using **only OTP/BEAM standard libraries** (Erlang/Elixir), **ETS for the in-memory index**, and a **multi-node** deployment model.

> We’ll mirror the Bitcask approach: append-only log files, an in-RAM “keydir” (our ETS index) that maps key → {file\_id, offset, value\_size, timestamp}, immutable closed segments, and background compaction with hint files for fast restart.&#x20;

---

# Step-by-step development plan

## Phase 0 — Ground rules & repo setup

1. **Constraints & dependencies**

   * Only OTP/BEAM standard apps: `:kernel`, `:stdlib`, `:crypto`, `:inets` (optional), `:logger`, `:os_mon`, `:erts`.
   * I/O via `:file` (`open`, `write`, `pread`, `sync`), sockets via `:gen_tcp` (or `:inets` httpd if you want HTTP), ETS for the index.
2. **Repo skeleton**

   * `apps/kv/` (OTP app): storage engine, index, compactor, network server.
   * `rel/` for a simple release script; `README.md` with run instructions & protocol.

## Phase 1 — On-disk data format (append-only)

3. **Record format (per entry)**

   * Header (fixed): magic (4B), version (1B), timestamp (8B), key\_len (4B), val\_len (8B), checksum (4B).
   * Payload: key (key\_len), value (val\_len). For **Delete**, write a tombstone: `val_len=0` and a special flag bit in header.
   * Checksum: CRC32 (via `:erlang.crc32/1` in modern OTP) or `:crypto.hash(:sha256, ...) |> binary_part(0,4)`.
4. **Segment layout**

   * Directory = one Bitcask “database”.
   * One **active** segment file (append-only). Rotate at size threshold (configurable).
   * Closed segments are **immutable**; keep `*.data`. For each closed `N.data` create `N.hint`.

## Phase 2 — Low-level I/O module

5. **Writer**

   * `:file.open/2` with `[:raw, :binary, :append, :delayed_write]` and optional `:sync` policy.
   * Batch iodata writes for **BatchPut** to minimize syscalls; `:file.sync/1` at end depending on durability mode.
6. **Reader**

   * `:file.pread/3` with offset/length from ETS metadata for O(1) single seek per read.
7. **Rotation**

   * When active segment > threshold, `:file.close`, mark immutable, write `*.hint`, open new active.

## Phase 3 — ETS “keydir” index

8. **Index tables**

   * `:ets.new(:keydir, [:set, :public, read_concurrency: true, write_concurrency: true])`
   * Value = `{file_id :: integer, offset :: non_neg_integer, size :: non_neg_integer, ts :: integer, tombstone? :: boolean}`
   * Optional: a second `:ordered_set` ETS (keys only) to accelerate **ReadKeyRange**.
   * Note: As in Bitcask, **keydir must fit in RAM**; values reside on disk, enabling datasets >> RAM.&#x20;
9. **Atomic updates**

   * On **Put/Delete**, append to active file then update ETS in the same server process; expose consistent reads.

## Phase 4 — Hint files & fast startup

10. **Hint writer (on rotation/merge)**

    * For each live key copied to a closed segment, write a compact `*.hint` entry: `{key, file_id, offset, size, ts}`.
11. **Startup index rebuild**

    * If hint exists: scan `*.hint` only (fast). Else: scan the `*.data` file end-to-start to rebuild ETS.
    * Truncate **partial tail** (if crash) by verifying header lengths & checksum; stop at first invalid boundary.

## Phase 5 — Storage engine OTP supervision

12. **Process model**

    * `Storage.Supervisor`

      * `Storage.Engine` (`GenServer`): owns active file handle, index mutation, rotation.
      * `Storage.FileCache`: manages a small LRU of open closed-segment FDs.
      * `Storage.Compactor` (`GenServer`): background merge.
13. **Config toggles**

    * `segment_max_bytes`, `sync_on_put` (always, never, every\_n\_ms), `merge_trigger` (count/ratio), `merge_throttle` (IO budget), `tombstone_ttl`.

## Phase 6 — API surface (in-process)

14. **Functions to expose**

    * `put(key, value)` → append, ETS update, optional fsync.
    * `get(key)` → ETS lookup → `:file.pread`.
    * `range(start_key, end_key)` → scan `:ordered_set` ETS keys in range; `pread` per hit.
    * `batch_put([{k,v}, ...])` → build single iolist append; single sync (configurable).
    * `delete(key)` → append tombstone; ETS delete or mark tombstoned.

## Phase 7 — Network protocol & server

15. **Choose protocol**

    * **Option A (recommended for low latency)**: **custom line/binary protocol** via `:gen_tcp`.

      * Example: `PUT <klen> <vlen>\n<key><value>`; `GET <klen>\n<key>`; `RANGE <slen> <elen>\n<start><end>`; `BPUT <n> ...`; `DEL <klen>\n<key>`.
    * **Option B**: basic HTTP with `:inets` httpd + JSON. Simpler for demos, slightly more overhead.
16. **Server design**

    * `Network.Acceptor` (listens with `:gen_tcp.listen`).
    * Spawn per-connection `GenServer` using `{active, :once}`, binary mode, backpressure friendly.
    * Pipeline safe: parse request → call storage API → write response; avoid long blocking in socket process.

## Phase 8 — Background compaction (merge)

17. **Trigger policy**

    * When stale\_ratio in closed files > threshold or total closed bytes > limit, start **merge**.
18. **Merge algorithm (immutable inputs)**

    * Stream keys from oldest → newest closed segments.
    * Keep only the latest non-tombstoned version per key; copy into a **new output segment**; build its hint file.
    * After success: atomically rename outputs; update ETS entries for moved keys; delete old segments.
19. **Throttling & predictability**

    * Limit bytes/s (sleep between batches), yield CPU, maintain strict IO budget to keep tail latencies stable.

## Phase 9 — Crash safety & durability

20. **Write ordering**

    * Append record → `:file.sync` per policy. With `sync_on_put=true`, you can claim no data loss on OS crash post-ack.
21. **Recovery**

    * On open: rebuild ETS from hints/data; drop torn records; checkpoint last clean offset per segment.
22. **Testing**

    * Kill -9 during floods; verify no silent corruption and fast reopen (seconds, not minutes).

## Phase 10 — High-throughput path

23. **BatchPut optimization**

    * Accumulate iodata for N puts (or ≤ X bytes) then **single `:file.write`** and **single `:file.sync`**.
24. **Read caching**

    * Rely on OS page cache (Bitcask does this); optionally a tiny LFU for hot small values if needed (ETS or `:persistent_term`), but start simple.&#x20;

## Phase 11 — Range scans (ReadKeyRange)

25. **Index for ranges**

    * Maintain `:ets` **ordered\_set** of keys (key → true). Range = `:ets.next/2` walk from `start_key` until `end_key`.
    * Trade-off: additional RAM usage for keys; values still on disk. Document the limit & tuning.

## Phase 12 — Observability & backpressure

26. **Metrics & logs**

    * Use `:logger`; counters via `:erlang.statistics/1` and simple internal counters (per-second snapshots).
27. **Backpressure**

    * Limit per-conn in-flight ops; if compactor busy or disk queue high, respond with `TRY_LATER` or slow-start.
    * Network loop with `{active, :once}` to avoid mailbox blowups.

## Phase 13 — Multi-node replication (bonus)

28. **Cluster membership**

    * Nodes discover peers via static config (initially) or DNS SRV; track with `:pg` or `:global`.
29. **Leader election (simple, stdlib-only)**

    * Use `:global.set_lock({:kv, :leader}, …)`; winner acts as **primary**. Others become **followers**.
    * Liveness via node monitors (`:net_kernel.monitor_nodes(true)`).
30. **Replication protocol**

    * **Log shipping** over TCP: followers maintain `{file_id, offset}` checkpoint.
    * Primary streams appended records; followers append to their own segments and update ETS, **fsync** per follower policy.
    * On reconnect, follower requests from last checkpoint.
31. **Consistency levels (config)**

    * `WRITE_LOCAL`: ack after local fsync (fast, possible loss if primary dies before replication).
    * `WRITE_MAJORITY`: primary waits for ≥ quorum follower fsync acks before acking client (slower, safer).
32. **Automatic failover**

    * If primary lost (node down, lock released), followers race for the lock; **winner becomes primary**.
    * To avoid split-brain, use **quorum write mode** in prod; otherwise accept potential last-N-writes loss on failover.
33. **Anti-entropy**

    * Periodic checksum of segment manifests; request missing ranges to heal followers (background).

## Phase 14 — CLI, config & docs

34. **Config**

    * `config.exs`: data dir, segment size, fsync policy, merge policy, network port, replication peers, quorum mode.
35. **CLI**

    * `bin/kv start …`, `bin/kv attach`, `bin/kv admin merge-now`, `bin/kv stats`.
36. **Protocol doc & README**

    * Describe API, network protocol, durability modes, merge behavior, operational playbook, and **how to run** locally and in multi-node mode.

## Phase 15 — Bench & validation

37. **Correctness**

    * Property tests (even without external libs: random workloads in an ExUnit test) for put/get/range/delete invariants.
38. **Performance**

    * Scripts to generate random K/V workloads; report throughput/latency; test with dataset >> RAM to validate stable behavior (keydir in RAM, values on disk).&#x20;
39. **Failure drills**

    * Kill primary mid-batch; power-off simulations; verify restart time (hint scan), replication catch-up, and data safety per policy.

---

## Key design trade-offs (why this meets the goals)

* **Low latency, high write throughput:** append-only segments + batched iodata + optional group fsyncs.
* **Datasets >> RAM:** only the **index** (keydir) and optional key set live in RAM; **values stay on disk**; reads are single `pread`.&#x20;
* **Crash friendliness:** the **data file is the commit log**; recovery = scan hints/data, drop torn tails; hint files make startup fast.&#x20;
* **Predictable under load:** immutable segments, background merges with throttling, connection backpressure.
* **Replication & failover:** stdlib-only leader election, log shipping, and optional quorum acks for stronger guarantees.

---

## What to put in the GitHub repo

* `/apps/kv/` OTP app as above.
* `/docs/` with:

  * **Architecture overview** (this plan).
  * **On-disk format** (header fields, endianness).
  * **Network protocol** (request/response frames).
  * **Operational guide** (merge policies, fsync modes, replication).
  * **Quickstart**: single-node and 3-node cluster instructions.
* `/scripts/` simple workload generators and failure test scripts.
