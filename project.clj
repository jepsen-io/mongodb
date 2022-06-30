(defproject jepsen.mongodb "0.3.2-SNAPSHOT"
  :description "Jepsen MongoDB tests"
  :url "http://github.com/jepsen-io/mongodb"
  :license {:name "EPL-2.0 OR GPL-2.0-or-later WITH Classpath-exception-2.0"
            :url "https://www.eclipse.org/legal/epl-2.0/"}
  :dependencies [[org.clojure/clojure "1.11.1"]
                 [clj-wallhack "1.0.1"]
                 [jepsen "0.2.7-SNAPSHOT"]
                 [org.mongodb/mongodb-driver-sync "4.6.0"]]
  :main jepsen.mongodb
  :jvm-opts ["-Djava.awt.headless=true"]
  :repl-options {:init-ns jepsen.mongodb})
