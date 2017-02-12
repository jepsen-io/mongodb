# Jepsen MongoDB tests

Runs two types of tests against a MongoDB cluster: single-document compare-and-set, and document-per-element set insertion.

## Examples

```sh
# Usage
lein run
lein run test --help

# Short set test with write and read concern majority
lein run test -t set

# 100 second linearizability test with write concern "journaled" and "local" read concern
lein run -t register --time-limit 100 -w journaled -r local

# Use the mmapv1 storage engine
lein run test -t register -s mmapv1

# Pick a different tarball to install
lein run test -t register --tarball https://...foo.tar.gz
```

## Building and running as a single jar

```sh
lein uberjar
java -jar target/jepsen.mongodb-0.2.0-SNAPSHOT-standalone.jar test ...
```

## License

Copyright © 2015, 2016 Kyle Kingsbury & Jepsen, LLC

Distributed under the Eclipse Public License either version 1.0 or (at
your option) any later version.
