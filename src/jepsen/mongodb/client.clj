(ns jepsen.mongodb.client
  "Wraps the MongoDB Java client."
  (:require [clojure.walk :as walk]
            [clojure.tools.logging :refer [info warn]]
            [jepsen [util :as util :refer [timeout]]]
            [slingshot.slingshot :refer [try+ throw+]])
  (:import (java.util ArrayList
                      List)
           (java.util.concurrent TimeUnit)
           (com.mongodb Block
                        ConnectionString
                        MongoClientSettings
                        MongoClientSettings$Builder
                        ServerAddress
                        WriteConcern
                        ReadConcern
                        ReadPreference)
           (com.mongodb.client MongoClient
                               MongoClients
                               MongoCollection
                               MongoDatabase
                               TransactionBody)
           (com.mongodb.client.model Filters
                                     FindOneAndUpdateOptions
                                     ReplaceOptions
                                     ReturnDocument
                                     Sorts
                                     Updates
                                     UpdateOptions)
           (com.mongodb.client.result UpdateResult)
           (com.mongodb.session ClientSession)
           (org.bson Document)))

(def mongos-port 27017)
(def shard-port  27018)
(def config-port 27019)

;; Basic node manipulation
(defn addr->node
  "Takes a node address like n1:27017 and returns just n1"
  [addr]
  ((re-find #"(\w+):\d+" addr) 1))

(defmacro with-block
  "Wrapper for the functional mongo Block interface"
  [x & body]
  `(reify Block
     (apply [_ ~x]
       ~@body)))

;; Connection management
(defn open
  "Opens a connection to a node."
  [node port]
  (MongoClients/create
    (.. (MongoClientSettings/builder)
        (applyToClusterSettings (with-block builder
                                  (.. builder
                                      (hosts [(ServerAddress. node port)])
                                      (serverSelectionTimeout 1 TimeUnit/SECONDS))))
        (applyToSocketSettings (with-block builder
                                 (.. builder
                                     (connectTimeout 5 TimeUnit/SECONDS)
                                     (readTimeout    5 TimeUnit/SECONDS))))
        (applyToConnectionPoolSettings (with-block builder
                                         (.. builder
                                             (minSize 1)
                                             (maxSize 1)
                                             (maxWaitTime 1 TimeUnit/SECONDS))))
        build)))

(defn await-open
  "Blocks until (open node) succeeds. Helpful for initial cluster setup."
  [node port]
  (timeout 30000
           (throw+ {:type ::timed-out-awaiting-connection
                    :node node
                    :port port})
           (loop []
             (or (try
                   (let [conn (open node port)]
                     (try
                       (.first (.listDatabaseNames conn))
                       conn
                       ; Don't leak clients when they fail
                       (catch Throwable t
                         (.close conn)
                         (throw t))))
                   (catch com.mongodb.MongoTimeoutException e
                     (info "Mongo timeout while waiting for conn; retrying. "
                           (.getMessage e))
                     nil)
                   (catch com.mongodb.MongoSocketReadTimeoutException e
                     (info "Mongo socket read timeout waiting for conn; retrying")
                     nil))
                 ; If we aren't ready, sleep and retry
                 (do (Thread/sleep 1000)
                     (recur))))))

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
  "Turns a named (e.g. :majority, \"majority\") into a WriteConcern."
  [wc]
  (when wc
    (case (name wc)
      "acknowledged"    WriteConcern/ACKNOWLEDGED
      "journaled"       WriteConcern/JOURNALED
      "majority"        WriteConcern/MAJORITY
      "unacknowledged"  WriteConcern/UNACKNOWLEDGED)))

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
      "snapshot"        ReadConcern/SNAPSHOT)))

(defn transactionless-read-concern
  "Read concern SNAPSHOT isn't supported outside transactions; we weaken it to
  MAJORITY."
  [rc]
  (case rc
    "snapshot" "majority"
    rc))

;; Error handling
(defmacro with-errors
  "Remaps common errors; takes an operation and returns a :fail or :info op
  when a throw occurs in body."
  [op & body]
  `(try ~@body
     (catch com.mongodb.MongoNotPrimaryException e#
       (assoc ~op :type :fail, :error :not-primary))

     (catch com.mongodb.MongoNodeIsRecoveringException e#
       (assoc ~op :type :fail, :error :node-recovering))

     (catch com.mongodb.MongoSocketReadTimeoutException e#
       (assoc ~op :type :info, :error :socket-read-timeout))

     (catch com.mongodb.MongoTimeoutException e#
       (condp re-find (.getMessage e#)
         #"Timed out after \d+ ms while waiting to connect"
         (assoc ~op :type :fail, :error :connect-timeout)

         (assoc ~op :type :info, :error :mongo-timeout)))

     (catch com.mongodb.MongoCommandException e#
       (condp re-find (.getMessage e#)
         #"TransactionCoordinatorSteppingDown"
         (assoc ~op :type :fail, :error :transaction-coordinator-stepping-down)

         (throw e#)))

     (catch com.mongodb.MongoClientException e#
       (condp re-find (.getMessage e#)
         ; This... seems like a bug too
         #"Sessions are not supported by the MongoDB cluster to which this client is connected"
         (assoc ~op :type :fail, :error :sessions-not-supported-by-cluster)

         (throw e#)))

     (catch com.mongodb.MongoQueryException e#
       (condp re-find (.getMessage e#)
         ; Why are there two ways to report this?
         #"code 10107 " (assoc ~op :type :fail, :error :not-primary-2)

         #"code 13436 " (assoc ~op :type :fail, :error :not-primary-or-recovering)
         (throw e#)
         ))))

(defn ^MongoDatabase db
  "Get a DB from a connection. Options may include

  :write-concern    e.g. :majority
  :read-concern     e.g. :local"
  ([conn db-name]
   (.getDatabase conn db-name))
  ([conn db-name opts]
   (let [rc (read-concern (:read-concern opts))
         wc (write-concern (:write-concern opts))]
     (cond-> (db conn db-name)
       rc (.withReadConcern rc)
       wc (.withWriteConcern wc)))))

(defn ^MongoCollection collection
  "Gets a Mongo collection from a DB."
  [^MongoDatabase db collection-name]
  (.getCollection db collection-name))

(defn create-collection!
  [^MongoDatabase db collection-name]
  (.createCollection db collection-name))

;; Sessions

(defn start-session
  "Starts a new session"
  [conn]
  (.startSession conn))

;; Transactions

(defmacro txn
  "Converts body to a TransactionBody function."
  [& body]
  `(reify TransactionBody
     (execute [this]
       ~@body)))

;; Actual commands

(defn command!
  "Runs a command on the given db."
  [^MongoDatabase db cmd]
  (parse (.runCommand db (->doc cmd))))

(defn admin-command!
  "Runs a command on the admin database."
  [conn cmd]
  (command! (db conn "admin") cmd))

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
