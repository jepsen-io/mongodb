(ns jepsen.mongodb.client
  "Wraps the MongoDB Java client."
  (:require [clojure.walk :as walk]
            [clojure.tools.logging :refer [info warn]]
            [dom-top.core :refer [assert+]]
            [jepsen [util :as util :refer [timeout]]]
            [slingshot.slingshot :refer [try+ throw+]]
            [wall.hack :as hack])
  (:import (java.io Closeable)
           (java.util ArrayList
                      List)
           (java.util.concurrent TimeUnit)
           (com.mongodb Block
                        ConnectionString
                        MongoClientException
                        MongoClientSettings
                        MongoClientSettings$Builder
                        MongoConnectionPoolClearedException
                        MongoQueryException
                        MongoSocketReadException
                        MongoSocketReadTimeoutException
                        MongoTimeoutException
                        MongoWriteConcernException
                        ReadConcern
                        ReadPreference
                        ServerAddress
                        TransactionOptions
                        WriteConcern)
           (com.mongodb.client ClientSession
                               MongoClient
                               MongoClients
                               MongoCollection
                               MongoDatabase
                               TransactionBody)
           (com.mongodb.client.internal ClientSessionImpl
                                        ClientSessionImpl$TransactionState)
           (com.mongodb.client.model Filters
                                     FindOneAndUpdateOptions
                                     ReplaceOptions
                                     ReturnDocument
                                     Sorts
                                     Updates
                                     UpdateOptions)
           (com.mongodb.client.result UpdateResult)
           (com.mongodb.internal.connection
             MongoWriteConcernWithResponseException)
           (com.mongodb.internal.session BaseClientSessionImpl
                                         ServerSessionPool
                                         ServerSessionPool$ServerSessionImpl)
           (com.mongodb.session ServerSession)
           (org.bson BsonBinary
                     Document)))

(def mongos-port 27017)
(def shard-port  27018)
(def config-port 27019)
(def proxy-port  7901)

(defprotocol Conn
  "We're juggling mongod, mongos, and possibly a proxy server in front of that.
  This protocol lets us tell what host and port clients should use to connect
  to something. We implement this on each DB."
  (host [this test node] "What host should we use to connect to this DB?")
  (port [this test] "What port should we use to connect to this DB?"))

(defn close!
  "Closes any Closeable."
  [^Closeable c]
  (.close c))

;; Basic node manipulation
(defn addr->node
  "Takes a node address like n1:27017 and returns just n1"
  [addr]
  ((re-find #"(.+):\d+" addr) 1))

(defmacro with-block
  "Wrapper for the functional mongo Block interface"
  [x & body]
  `(reify Block
     (apply [_ ~x]
       ~@body)))

;; Connection management
(defn config-server?
  "Takes a test map, and returns true iff this set of nodes is intended to be a
  configsvr--e.g. if it has a :replica-set-name of 'rs_config'."
  [test]
  (= (:replica-set-name test) "rs_config"))

(defn ^MongoClient open
  "Opens a connection to a node. Second arg can be either a test map (in which
  case the port is derived from (port (:db test)) or an integer (in which case
  it's used directly."
  [node test-or-port]
  (let [host (if (integer? test-or-port)
               node
               (host (:db test-or-port) test-or-port node))
        port (if (integer? test-or-port)
               test-or-port
               (port (:db test-or-port) test-or-port))
        test (if (integer? test-or-port) {} test-or-port)]
    ;(info "Connecting to" (str host ":" port))
    (MongoClients/create
      (cond-> (.. (MongoClientSettings/builder)
                  (applyToClusterSettings
                    (with-block builder
                      (.. builder
                          (hosts [(ServerAddress. host port)])
                          (serverSelectionTimeout 1 TimeUnit/SECONDS))))
                  (applyToSocketSettings
                    (with-block builder
                      (.. builder
                          (connectTimeout 5 TimeUnit/SECONDS)
                          (readTimeout    5 TimeUnit/SECONDS))))
                  (applyToConnectionPoolSettings
                    (with-block builder
                      (.. builder
                          (minSize 1)
                          (maxSize 1)
                          (maxWaitTime 1 TimeUnit/SECONDS)))))

        (boolean? (:retry-writes test))
        (.retryWrites (:retry-writes test))

        true (.build)))))

(declare ping)

(defn ^MongoClient await-open*
  "Blocks until (open node) succeeds, and optionally pings."
  [node test-or-port ping?]
  (let [port (if (integer? test-or-port)
               test-or-port
               (port (:db test-or-port) test-or-port))]
    (assert+ (integer? port) {:type ::not-integer-port
                              :db   (when-not (integer? test-or-port)
                                      (:db test-or-port))
                              :port port})
    (util/await-fn
      (fn conn []
        (try+
          (let [conn (open node test-or-port)]
            (try
              (when ping?
                ;(.first (.listDatabaseNames conn))
                (ping conn))
              conn
              ; Don't leak clients when they fail
              (catch Throwable t
                (.close conn)
                (throw t))))
          (catch com.mongodb.MongoTimeoutException e
            (info "Mongo timeout while waiting for conn; retrying."
                  (.getMessage e))
            (throw+ {:type ::timed-out-awaiting-connection
                     :node node
                     :port port}))
          (catch com.mongodb.MongoNodeIsRecoveringException e
            (info "Node is recovering; retrying." (.getMessage e))
            (throw+ {:type :node-recovering-awaiting-connection
                     :node node
                     :port port}))
          (catch com.mongodb.MongoSocketReadTimeoutException e
            (info "Mongo socket read timeout waiting for conn; retrying")
            (throw+ {:type :mongo-read-timeout-awaiting-connection
                     :node node
                     :port port}))))
      {:retry-interval 1000
       :log-interval   10000
       :log-message    (str "Waiting for " node ":" port " to be available")
       :timeout        300000})))

(defn ^MongoClient await-open
  "Blocks until (open node) succeeds and the server responds to ping. Helpful
  for initial cluster setup."
  [node port]
  (await-open* node port true))

; Basic plumbing
(defprotocol ToDoc
  "Supports coercion to MongoDB BSON Documents."
  (->doc [x]))

(extend-protocol ToDoc
  nil
  (->doc [_] (Document.))

  clojure.lang.Keyword
  (->doc [x] (name x))

  clojure.lang.IPersistentMap
  (->doc [x]
    (->> x
         (map (fn [[k v]] [(name k) (->doc v)]))
         (into {})
         (Document.)))

  clojure.lang.Sequential
  (->doc [x]
    (ArrayList. (map ->doc x)))

  Object
  (->doc [x] x))

(defprotocol FromDoc
  "Supports coercion from MongoDB BSON Documents"
  (parse [x]))

(extend-protocol FromDoc
  nil
  (parse [x] nil)

  Document
  (parse [x]
    (persistent!
      (reduce (fn [m [k v]]
                (assoc! m (keyword k) (parse v)))
              (transient {})
              (.entrySet x))))

  UpdateResult
  (parse [r]
    {:matched-count  (.getMatchedCount r)
     :modified-count (.getModifiedCount r)
     :upserted-id    (.getUpsertedId r)
     :acknowledged?  (.wasAcknowledged r)})

  List
  (parse [x]
    (map parse x))

  Object
  (parse [x]
    x))

;; Write Concerns
(defn write-concern
  "Turns a named (e.g. :majority, \"majority\") into a WriteConcern. Integer
  strings like \"2\" are converted to a WriteConcern as well. Optionally takes
  a journal option, which can be true (forces journaling), false (disables
                                                                   journaling),
  or nil (leaves as default)."
  ([wc]
   (write-concern wc nil))
  ([wc j]
   (let [wc (when wc
              (cond-> (case (name wc)
                        "acknowledged"    WriteConcern/ACKNOWLEDGED
                        "journaled"       WriteConcern/JOURNALED
                        "majority"        WriteConcern/MAJORITY
                        "unacknowledged"  WriteConcern/UNACKNOWLEDGED
                        (WriteConcern. (Integer/parseInt wc)))
                (not (nil? j)) (.withJournal j)))]
     wc)))

(defn read-concern
  "Turns a named (e.g. :majority, \"majority\" into a ReadConcern."
  [rc]
  (when rc
    (case (name rc)
      "available"       ReadConcern/AVAILABLE
      "default"         ReadConcern/DEFAULT
      "linearizable"    ReadConcern/LINEARIZABLE
      "local"           ReadConcern/LOCAL
      "majority"        ReadConcern/MAJORITY
      "snapshot"        ReadConcern/SNAPSHOT
      (ReadConcern. (Integer/parseInt rc)))))

(defn transactionless-read-concern
  "Read concern SNAPSHOT isn't supported outside transactions; we weaken it to
  MAJORITY."
  [rc]
  (case rc
    "snapshot" "majority"
    rc))

(defn ^ReadPreference read-preference
  "Turns a string or keyword read preference into a Mongo ReadPreference. nil
  is passed through."
  [pref]
  (when pref
    (ReadPreference/valueOf (name pref))))

;; Error handling
(defmacro with-errors
  "Remaps common errors; takes an operation and returns a :fail or :info op
  when a throw occurs in body."
  [op & body]
  `(try ~@body
     (catch MongoConnectionPoolClearedException e#
       (assoc ~op :type :fail, :error [:connection-pool-cleared
                                       (.getMessage e#)]))

     (catch com.mongodb.MongoNotPrimaryException e#
       (assoc ~op :type :fail, :error :not-primary))

     (catch com.mongodb.MongoNodeIsRecoveringException e#
       (assoc ~op :type :fail, :error :node-recovering))

     (catch MongoSocketReadException e#
       (assoc ~op :type :info, :error [:socket-read-exception
                                       (.getMessage e#)]))

     (catch MongoSocketReadTimeoutException e#
       (assoc ~op :type :info, :error :socket-read-timeout))

     (catch com.mongodb.MongoTimeoutException e#
       (condp re-find (.getMessage e#)
         #"Timed out after \d+ ms while waiting to connect"
         (assoc ~op :type :fail, :error :connect-timeout)

         ; What was this message?
         ;(assoc ~op :type :info, :error :mongo-timeout)

         (throw e#)))

     (catch com.mongodb.MongoExecutionTimeoutException e#
       (assoc ~op :type :info, :error :mongo-execution-timeout))

     (catch com.mongodb.MongoWriteException e#
       (condp re-find (.getMessage e#)
         #"Not primary so we cannot begin or continue a transaction"
         (assoc ~op :type :fail, :error :not-primary-cannot-txn)

         ; This LOOKS like it ought to be a definite failure, but it's not!
         ; Write transactions can throw this but actually succeed. I'm calling
         ; it info for now.
         #"Could not find host matching read preference"
         (assoc ~op :type :info, :error :no-host-matching-read-preference)

         (throw e#)))

     (catch MongoWriteConcernException e#
       (condp re-find (.getMessage e#)
         #"operation was interrupted"
         (assoc ~op :type :fail, :error :write-concern-interrupted)

         #"Primary stepped down while waiting for replication"
         (assoc ~op :type :fail, :error :primary-stepped-down-waiting-for-replication)

         (throw e#)))

     (catch com.mongodb.MongoCommandException e#
       (condp re-find (.getMessage e#)
         #"error 133 "
         (assoc ~op :type :fail, :error [:failed-to-satisfy-read-preference
                                         (.getMessage e#)])

         #"WriteConflict"
         (assoc ~op :type :fail, :error :write-conflict)

         ; Huh, this is NOT, as it turns out, a determinate failure.
         #"TransactionCoordinatorSteppingDown"
         (assoc ~op :type :info, :error :transaction-coordinator-stepping-down)

         ; This can be the underlying cause of issues like "unable to
         ; initialize targeter for write op for collection..."
         ; These are ALSO apparently not... determinate failures?
         #"Connection refused"
         (assoc ~op :type :info, :error :connection-refused)

         ; Likewise
         #"Connection reset by peer"
         (assoc ~op :type :info, :error :connection-reset-by-peer)

         (throw e#)))

     (catch MongoClientException e#
       (condp re-find (.getMessage e#)
         ; This... seems like a bug too
         ; Can also happen when connecting to a hidden replica
         #"Sessions are not supported by the MongoDB cluster to which this client is connected"
         (do (Thread/sleep 5000)
             (assoc ~op :type :fail, :error :sessions-not-supported-by-cluster))

         (throw e#)))

     (catch MongoQueryException e#
       (condp re-find (.getMessage e#)
         #"Could not find host matching read preference"
         (assoc ~op :type :fail, :error :no-host-matching-read-preference)

         #"code 251 " (assoc ~op :type :fail, :error :transaction-aborted)

         #"code 133 " (assoc ~op :type :fail,
                             :error [:failed-to-satisfy-read-preference
                                     (.getMessage e#)])

         ; Why are there two ways to report this?
         #"code 10107 " (assoc ~op :type :fail, :error :not-primary-2)

         #"code 11602 " (assoc ~op :type :info, :error :interrupted-due-to-repl-state-change)

         #"code 13436 " (assoc ~op :type :fail, :error :not-primary-or-recovering)
         (throw e#)))

     (catch MongoTimeoutException e#
       (condp re-find (.getMessage e#)
         ; If we timed out before getting a connection, we clearly can't have
         ; done anything.
         #"while waiting for a connection to server"
         (assoc ~op :type :fail, :error :timeout-waiting-for-connection)

         (throw e#)))

     (catch MongoWriteConcernWithResponseException e#
       (condp re-find (.getMessage e#)
         ; Not really clear whether this should be a definite failure or not,
         ; but writes that fail with this kind of error DO succeed on occasion,
         ; so let's call it info.
         #"InterruptedDueToReplStateChange"
         (assoc ~op :type :info, :error :interrupted-due-to-repl-state-change)
         (throw e#)))
     ))

(defn ^MongoDatabase db
  "Get a DB from a connection. Options may include

  :write-concern    e.g. :majority
  :read-concern     e.g. :local
  :read-preference  e.g. :secondary
  :journal          If present, forces journaling to be enabled or disabled
                    for write concerns."
  ([conn db-name]
   (.getDatabase conn db-name))
  ([conn db-name opts]
   (let [rc (read-concern    (:read-concern opts))
         rp (read-preference (:read-preference opts))
         wc (write-concern   (:write-concern opts) (:journal opts))]
     (cond-> (db conn db-name)
       rc (.withReadConcern rc)
       rp (.withReadPreference rp)
       wc (.withWriteConcern wc)))))

(defn ^MongoCollection collection
  "Gets a Mongo collection from a DB."
  [^MongoDatabase db collection-name]
  (.getCollection db collection-name))

(defn create-collection!
  [^MongoDatabase db collection-name]
  (.createCollection db collection-name))

;; Sessions

(defn ^ClientSession start-session
  "Starts a new session"
  [conn]
  (.startSession conn))

; The astute reader may ask: just *why* are we so pre-occupied with hacking our
; way into the guts of client & server session state and making one session
; look like another? Because, dearest reader, we intend to do something
; terrible but apparently not forbidden by the spec, and split a single
; transaction across *multiple* nodes with independent clients.

(defn ^ServerSession get-server-session
  "Takes a client session and extracts its corresponding server session."
  [^ClientSession client-session]
  (.getServerSession client-session))

(defn ^BaseClientSessionImpl set-server-session!
  "Takes a client session and sets its server session. Returns client session."
  [^BaseClientSessionImpl client-session ^ServerSession server-session]
  (let [field (.getDeclaredField BaseClientSessionImpl "serverSession")]
    (.setAccessible field true)
    (.set field client-session server-session))
  client-session)

(defn ^ClientSessionImpl$TransactionState txn-state
  "Retrieves the txn state from a client session."
  [^ClientSessionImpl session]
  (let [field (.getDeclaredField ClientSessionImpl "transactionState")]
    (.setAccessible field true)
    (.get field session)))

(defn ^ClientSessionImpl set-txn-state!
  "Sets the transactionState field of a client session, and returns it."
  [^ClientSessionImpl session ^ClientSessionImpl$TransactionState state]
  (let [field (.getDeclaredField ClientSessionImpl "transactionState")]
    (.setAccessible field true)
    (.set field session state))
  session)

(defn ^TransactionOptions txn-opts
  "Retrieves the transaction options from a client session."
  [^ClientSessionImpl session]
  (let [field (.getDeclaredField ClientSessionImpl "transactionOptions")]
    (.setAccessible field true)
    (.get field session)))

(defn ^ClientSessionImpl set-txn-opts!
  "Sets the transaction options field of a client session, and returns it."
  [^ClientSessionImpl session ^TransactionOptions opts]
  (let [field (.getDeclaredField ClientSessionImpl "transactionOptions")]
    (.setAccessible field true)
    (.set field session opts))
  session)

(defn ^ServerSessionPool$ServerSessionImpl
  set-server-session-impl-transaction-number!
  "Override a server session's transaction number. Returns the session."
  [^ServerSessionPool$ServerSessionImpl session ^long txn-no]
  (let [field (.getDeclaredField ServerSessionPool$ServerSessionImpl
                                 "transactionNumber")]
    (.setAccessible field true)
    (.setLong field session txn-no))
  session)

(defn ^ServerSessionPool server-session->server-session-pool
  "Extracts the server session pool from a ServerSessionImpl. This is stored in
  the implicit instance variable this$0."
  [^ServerSessionPool$ServerSessionImpl session]
  (let [field (.getDeclaredField ServerSessionPool$ServerSessionImpl "this$0")]
    (.setAccessible field true)
    (.get field session)))

(defn clone-server-session
  "Takes a server session and returns a copy of it with the same session
  identifier and transaction number."
  [^ServerSessionPool$ServerSessionImpl session]
  (let [; First, extract the session identifier from the original session
        session-id (.getIdentifier session)
        identifier (.get session-id "id")
        ; And the transaction number.
        txn-no     (.getTransactionNumber session)
        ; We also need the server session pool so we can make a new session
        pool       (server-session->server-session-pool session)
        ; Now construct a fresh session
        constructor (first (.getDeclaredConstructors
                             ServerSessionPool$ServerSessionImpl))
        _ (.setAccessible constructor true)
        session' (-> constructor
                     (.newInstance (into-array Object [pool identifier]))
                     (set-server-session-impl-transaction-number! txn-no))]
    session'))

(defmacro with-session-like
  "Takes a vector of a target client session and a source client session.
  Within body, client session will have its session ID, txn number, transaction
  state, and transaction options, overridden to look like the source session,
  then reset at the end of the body."
  [[target source] & body]
  `(let [server-session#    (get-server-session ~target)
         server-session'#   (clone-server-session (get-server-session ~source))
         txn-state#         (txn-state ~target)
         txn-state'#        (txn-state ~source)
         txn-opts#          (txn-opts ~target)
         txn-opts'#         (txn-opts ~source)]
     (set-server-session! ~target server-session'#)
     (set-txn-state!      ~target txn-state'#)
     (set-txn-opts!       ~target txn-opts'#)
     (try ~@body
          (finally
            (set-server-session!  ~target server-session#)
            (set-txn-state!       ~target txn-state#)
            (set-txn-opts!        ~target txn-opts#)))))

(defn notify-message-sent!
  "Notifies a session that a message was sent in this transaction. We have to
  force this when we're threading transactions across multiple sessions."
  [^ClientSessionImpl session]
  (.notifyMessageSent session))

;; Transactions

(defmacro txn
  "Converts body to a TransactionBody function."
  [& body]
  `(reify TransactionBody
     (execute [this]
       ~@body)))

(defn start-txn!
  "Starts a txn on a session with the given transaction options."
  [^ClientSession session ^TransactionOptions opts]
  (.startTransaction session opts))

(defn commit-txn!
  [^ClientSession session]
  (.commitTransaction session))

;; Actual commands

(defn command!
  "Runs a command on the given db."
  [^MongoDatabase db cmd]
  (parse (.runCommand db (->doc cmd))))

(defn admin-command!
  "Runs a command on the admin database."
  [conn cmd]
  (command! (db conn "admin") cmd))

(defn ping
  "Pings the server with a default database."
  [conn]
  (admin-command! conn {:ping 1}))

(defn find-one
  "Find a document by ID. If a session is provided, will use that session
  for a causally consistent read"
  ([coll id]
   (find-one coll nil id))
  ([^MongoCollection coll ^ClientSession session id]
   (let [filt (Filters/eq "_id" id)]
     (-> (if session
           (.find coll session filt)
           (.find coll filt))
         .first
         parse))))

(defn upsert!
  "Ensures the existence of the given document, a map with at minimum an :_id
  key."
  ([^MongoCollection coll doc]
   (upsert! nil coll doc))
  ([^ClientSession session ^MongoCollection coll doc]
   (assert (:_id doc))
   (parse
     (if session
       (.replaceOne coll
                    session
                    (Filters/eq "_id" (:_id doc))
                    (->doc doc)
                    (.upsert (ReplaceOptions.) true))
       (.replaceOne coll
                    (Filters/eq "_id" (:_id doc))
                    (->doc doc)
                    (.upsert (ReplaceOptions.) true))))))
