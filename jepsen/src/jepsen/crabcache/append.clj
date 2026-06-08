(ns jepsen.crabcache.append
  "The gold-standard workload, ported from jepsen-io/redis: list-append with Elle.

   Each transaction is a list of micro-ops — `[:append k v]` (RPUSH) or `[:r k]`
   (LRANGE k 0 -1) — wrapped in MULTI/EXEC so the whole transaction commits as one
   atomic crab-cache EXEC-TXN Raft entry. Elle then looks for cycles in the
   read/write/append dependency graph, detecting anomalies up to STRICT
   SERIALIZABILITY. This is the same workload Kyle Kingsbury ran against Redis-Raft,
   so crab-cache's results are directly comparable.

   Requires crab-cache MULTI/EXEC (cc-btk.8)."
  (:require [jepsen [client :as client]]
            [jepsen.tests.cycle.append :as append]
            [jepsen.crabcache.client :as cc]
            [taoensso.carmine :as car]))

(defn- mop->cmd
  "Micro-op -> raw RESP command vector."
  [[f k v]]
  (case f
    :append ["RPUSH"  (str k) (str v)]
    :r      ["LRANGE" (str k) "0" "-1"]))

(defn- fill-reads
  "Zip the EXEC result array back onto the mops: :r gets its read list (parsed to
   longs), :append is unchanged."
  [mops exec-arr]
  (mapv (fn [[f k _ :as mop] res]
          (if (= f :r)
            [:r k (mapv #(Long/parseLong (str %)) res)]
            mop))
        mops exec-arr))

(defrecord Client [node]
  client/Client
  (open! [this test n] (assoc this :node (atom (name n))))
  (setup! [this test])

  (invoke! [this test op]
    (let [mops (:value op)
          ;; One MULTI ... EXEC pipeline; exec-cmd follows MOVED/TRYAGAIN and
          ;; retries the WHOLE transaction (safe: a redirected/rejected EXEC never
          ;; applied). (last ...) is the EXEC reply: the array of per-mop results.
          reply (cc/exec-cmd (:node this)
                  (fn [conn]
                    (last (car/wcar conn
                            (car/redis-call ["MULTI"])
                            (doseq [m mops] (car/redis-call (mop->cmd m)))
                            (car/redis-call ["EXEC"])))))]
      (if (sequential? reply)
        (assoc op :type :ok :value (fill-reads mops reply))
        ;; nil = aborted EXEC (shouldn't happen without WATCH); definitely not
        ;; applied, so :fail (not :info).
        (assoc op :type :fail, :error :exec-not-array))))

  (teardown! [this test])
  (close! [this test]))

(defn workload
  "Elle list-append workload. opts may carry :key-count / :max-txn-length."
  [opts]
  (assoc (append/test {:key-count          (:append-keys opts 8)
                       :min-txn-length     1
                       :max-txn-length     (:append-txn-len opts 4)
                       :max-writes-per-key 32
                       :consistency-models [:strict-serializable]})
         :client (->Client nil)))
