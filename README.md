# jepsen.mongodb

Tests for MongoDB.

## Usage

### For docker based tests

1. Follow the instructions [here](https://github.com/jepsen-io/jepsen/tree/main/docker) to get the docker cluster running
2. Once you have it running, shut it down and modify the `jepsen/docker/docker-compose.yml` file to add 2 additional nodes. Running these tests require more than 6 MongodDB nodes to be running. You can add 2 more using code snippet below
```yml
services:
  control:
    container_name: jepsen-control
    hostname: control
    depends_on:
      - n1
      - n2
      - n3
      - n4
      - n5
+     - n6
+     - n7

...
...
# Add these lines
  n6:
    << : *default-node
    container_name: jepsen-n6
    hostname: n6
  n7:
    << : *default-node
    container_name: jepsen-n7
    hostname: n7

```
3. It also helps to mount the source code at an accessible location from within the control node. For that, do this
```yml
services:
  control:
    container_name: jepsen-control
    ...
    ...
    networks:
      - jepsen
+   volumes:
+     - "<path-from-this-dir-to-mongodb-repo>:/usr/share/code:rw"

```
4. Start the cluster again.
5. SSH into the control node and create a nodes file at an accessible location. The home folder is just fine. Add this to the nodes file
```
n1
n2
n3
n4
n5
n6
n7
```
6. Navigate into the mounted location where you have this repository. If you have followed the above instructions it should be `/usr/share/code`. Run the tests using below command
```shell
lein run test-all -w list-append --nodes-file ~/nodes-file -r 1000 --concurrency 3n --time-limit 120 --max-writes-per-key 128 --read-concern majority --write-concern majority --txn-read-concern snapshot --txn-write-concern majority --nemesis-interval 1 --nemesis partition --test-count 1
```

### For other setups

```shell
lein run test-all -w list-append --nodes-file ~/nodes -r 1000 --concurrency 3n --time-limit 120 --max-writes-per-key 128 --read-concern majority --write-concern majority --txn-read-concern snapshot --txn-write-concern majority --nemesis-interval 1 --nemesis partition --test-count 30
```


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
