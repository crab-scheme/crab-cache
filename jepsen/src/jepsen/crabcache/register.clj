(ns jepsen.crabcache.register
  "Linearizable register workload: many independent keys, each a read/write
   register, checked by Knossos for linearizability.

   crab-cache has no WATCH/MULTI/EXEC, so we cannot offer a `cas` op (that needs
   optimistic compare-and-set). A read/write-only register is a *weaker* test than
   the classic cas register, but it still catches the failures we care about under
   faults: stale reads, lost acknowledged writes, and split-brain (a read on one
   side returning a value the other side has already overwritten). Adding
   WATCH+MULTI/EXEC to crab-cache would unlock both a cas register and the full
   jepsen-io/redis list-append (Elle, strict-serializable) workload."
  (:require [jepsen [checker :as checker]
                    [client :as client]
                    [generator :as gen]
                    [independent :as independent]]
            [jepsen.crabcache.client :as cc]
            [knossos.model :as model]
            [taoensso.carmine :as car]))

(defn- ->long [s] (when s (Long/parseLong (str s))))

(def op-read  (fn [_ _] {:type :invoke, :f :read,  :value nil}))
(def op-write (fn [_ _] {:type :invoke, :f :write, :value (rand-int 5)}))

(defrecord Client [node]
  client/Client
  (open! [this test n]
    ;; `node` becomes a mutable atom of the current target host, so MOVED
    ;; redirects (incl. after failover) stick for subsequent ops.
    (assoc this :node (atom (name n))))

  (setup! [this test])

  (invoke! [this test op]
    (let [[k v] (:value op)
          rk    (str "reg:" k)]
      (case (:f op)
        :read  (let [res (cc/exec-cmd (:node this)
                                      (fn [c] (car/wcar c (car/get rk))))]
                 (assoc op :type :ok :value (independent/tuple k (->long res))))
        :write (do (cc/exec-cmd (:node this)
                                (fn [c] (car/wcar c (car/set rk (str v)))))
                   (assoc op :type :ok)))))

  (teardown! [this test])
  (close! [this test]))

(defn workload
  "Register workload. opts may carry :register-keys (concurrent keys) and
   :register-ops (ops per key)."
  [opts]
  (let [group (:register-group opts 2)   ; worker threads PER KEY (per-key concurrency)
        ops   (:register-ops   opts 100)] ; ops per key
    {:client    (->Client nil)
     :checker   (independent/checker
                  (checker/linearizable {:model     (model/register)
                                         :algorithm :linear}))
     :generator (independent/concurrent-generator
                  group
                  (range)
                  (fn [_k]
                    (->> (gen/mix [op-read op-write])
                         (gen/limit ops))))}))
