# jepsen.mongodb

Tests for MongoDB.

## Usage

lein run test-all -w list-append --nodes-file ~/nodes -r 100 --concurrency 3n --time-limit 300 --max-writes-per-key 128 --nemesis partition --read-concern majority --write-concern majority --txn-read-concern snapshot --txn-write-concern majority --test-count 40; and-bell

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
