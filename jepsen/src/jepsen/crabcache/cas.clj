(ns jepsen.crabcache.cas
  "Linearizable cas-register workload — the strong register test. Each key is a
   register supporting read (GET), write (SET), and compare-and-set (the native
   atomic CAS command, one Raft entry). `cas` is what gives Knossos enough
   constraints to pin down a linearization, so this is a far stronger consistency
   probe than the plain read/write register.

   Requires crab-cache's CAS command."
  (:require [jepsen [client :as client]
                    [checker :as checker]
                    [generator :as gen]
                    [independent :as independent]]
            [jepsen.crabcache.client :as cc]
            [knossos.model :as model]
            [taoensso.carmine :as car]))

(defn- ->long [s] (when s (Long/parseLong (str s))))

(def op-read  (fn [_ _] {:type :invoke, :f :read,  :value nil}))
(def op-write (fn [_ _] {:type :invoke, :f :write, :value (rand-int 5)}))
(def op-cas   (fn [_ _] {:type :invoke, :f :cas,   :value [(rand-int 5) (rand-int 5)]}))

(defrecord Client [node]
  client/Client
  (open! [this test n] (assoc this :node (atom (name n))))
  (setup! [this test])

  (invoke! [this test op]
    (let [[k v] (:value op)
          rk    (str "cas:" k)]
      (case (:f op)
        :read  (assoc op :type :ok
                      :value (independent/tuple
                               k (->long (cc/exec-cmd (:node this)
                                           (fn [c] (car/wcar c (car/get rk)))))))
        :write (do (cc/exec-cmd (:node this) (fn [c] (car/wcar c (car/set rk (str v)))))
                   (assoc op :type :ok))
        :cas   (let [[old new] v
                     r (cc/exec-cmd (:node this)
                         (fn [c] (car/wcar c (car/redis-call ["CAS" rk (str old) (str new)]))))]
                 ;; CAS returns 1 (set) or 0 (current != old). 0 is a definite
                 ;; failure (no write happened), so :fail not :info.
                 (assoc op :type (if (= 1 (long r)) :ok :fail))))))

  (teardown! [this test])
  (close! [this test]))

(defn workload
  [opts]
  (let [group (:register-group opts 2)
        ops   (:register-ops   opts 100)]
    {:client    (->Client nil)
     :checker   (independent/checker
                  (checker/linearizable {:model     (model/cas-register)
                                         :algorithm :linear}))
     :generator (independent/concurrent-generator
                  group
                  (range)
                  (fn [_k]
                    (->> (gen/mix [op-read op-write op-cas])
                         (gen/limit ops))))}))
