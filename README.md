# jepsen.mongodb

Tests for MongoDB, running on Debian Buster. You want 9 nodes for multi-shard
tests: 3 for the config replica set, and 3 for each of 2 shards.

## Usage

```
lein run test-all -w list-append --nodes-file ~/nodes -r 1000 --concurrency 3n --time-limit 120 --max-writes-per-key 128 --read-concern majority --write-concern majority --txn-read-concern snapshot --txn-write-concern majority --nemesis-interval 1 --nemesis partition --test-count 30
```

## Workloads

`list-append` performs a mix of non-transactional and transactional appends and reads to documents by primary key.
`list-append-multi-node` tries to do the same, but splitting requests across multiple nodes. This does not work yet, and may never.

## Nemeses

`partition`, `kill`, and `pause` create/resolve network partitions,
kill/restart processes with `kill -9`, and pause processes with
`SIGSTOP/SIGCONT`. `clock` adjusts the clock by anywhere from a few millis to
hundreds of seconds, and sometimes strobes the clock rapidly back and forth.
`member` adds and removes nodes dynamically.

## Options

See `lein run test --help` for all options.

`--hidden NUM` allows you to designate the first `NUM` nodes in each replica
set as hidden replicas.

`--[no-]journal` allows you to force write concern's `journal` flag on or off; otherwise it's left at the client default.

`--lazyfs` mounts Mongo's data directory on the lazyfs filesystem, which means
that process kills not only kill processes, but *also* lose un-fsynced writes.
Helpful for simulating power failures.

`--local-proxy PATH/TO/LOCAL/BIN` runs a custom binary on the local control
node, and routes client requests to it rather than remote nodes directly.
`--proxy` does the same, but uploads and runs the proxy on each DB node. This
feature was developed for a Jepsen client whose proxy is not public, but it
ought to work with any proxy which takes the same arguments. See
`jepsen.mongodb.db.local-proxy/start!` for details.

`--max-txn-length` and `max-writes-per-key` govern the maximum size of
transactions (e.g. for the list-append workload) and the number of writes to
any single key before choosing a new key.

`--nemesis FAULTS` takes a comma-separated list of faults to inject, and
`--nemesis-interval SECONDS` controls roughly how long between nemesis
operations, for each class of fault.

These tests automatically set write concern even on read-only
transactions--consistent with MongoDB's documentation. However, doing this is
not intuitive and many guides to Mongo transactions omit it. Use
`--no-read-only-write-concern` to do the obvious, wrong thing and *not* provide
a write concern for read-only transactions. This causes snapshot isolation
violations.

`--rate HZ` controls the upper bound on how many operations per second Jepsen
tries to perform.

`--read-concern CONCERN` and `--txn-read-concern CONCERN` set the read concern
for single operations and Mongo transactions, respectively. `--write-concern`
  and `--txn-write-concern` do the same for write concerns. If omitted, uses
  client defaults, which are totally unsafe.

`--read-preference PREF` controls whether the client tries to read from
primaries, secondaries, etc.

`--[no-]retry-writes` allows you to explicitly choose whether or not to
enable retryable writes at the client level. Note that Mongo ignores this
setting for some transaction features.

`--sharded`, if set, runs multiple replica sets, each with 3 replicas. The
first three nodes form one replica set, used for config. The second 3 nodes
form the first data shard, the third 3 nodes form the second data shard, and so
on--you therefore want at least 9 nodes to run a sharded test.

This test only performs Mongo transactions if the logical Jepsen transaction
has multiple operations--for instance, a read and a write or three writes.
Single reads and single writes are executed directly, without a Mongo
transaction. Use `--singleton-txns` to force a Mongo transaction for *every*
Jepsen transaction.

`-v VERSION` controls which MongoDB version we install and test.

`-w WORKLOAD` tells Jepsen which workload to run: e.g. `list-append`.

## License

Copyright © 2020 Jepsen, LLC

This program and the accompanying materials are made available under the
terms of the Eclipse Public License 2.0 which is available at
http://www.eclipse.org/legal/epl-2.0.

This Source Code may also be made available under the following Secondary
Licenses when the conditions for such availability set forth in the Eclipse
Public License, v. 2.0 are satisfied: GNU General Public License as published by
the Free Software Foundation, either version 2 of the License, or (at your
option) any later version, with the GNU Classpath Exception which is available
at https://www.gnu.org/software/classpath/license.html.
