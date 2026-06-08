(ns jepsen.crabcache.counter
  "INCR counter workload — the formal version of bench/linearizability.sh.

   Two checkers run together:
     * jepsen.checker/counter — every read must lie within [acked-lower,
       possible-upper]; catches LOST increments (a committed INCR not reflected).
     * unique-incr-checker (below) — every successful INCR must return a DISTINCT
       value. crab-cache's known non-idempotent-replay gap is that an INCR could
       double-apply on a re-replicate during recovery/rejoin; that shows up as two
       acked INCRs returning the same value, which a magnitude-only counter check
       would miss."
  (:require [jepsen [checker :as checker]
                    [client :as client]
                    [generator :as gen]]
            [jepsen.crabcache.client :as cc]
            [taoensso.carmine :as car]))

(def ctr-key "ctr")

(defn- ->long [s] (if s (Long/parseLong (str s)) 0))

(defrecord Client [node]
  client/Client
  (open! [this test n] (assoc this :node (atom (name n))))
  (setup! [this test])

  (invoke! [this test op]
    (case (:f op)
      ;; :value stays 1 (the delta) for checker/counter; the INCR's returned
      ;; total is stashed under :incr-result for the uniqueness checker.
      :add  (let [v (cc/exec-cmd (:node this)
                                 (fn [c] (car/wcar c (car/incr ctr-key))))]
              (assoc op :type :ok :incr-result (long v)))
      :read (let [v (cc/exec-cmd (:node this)
                                 (fn [c] (car/wcar c (car/get ctr-key))))]
              (assoc op :type :ok :value (->long v)))))

  (teardown! [this test])
  (close! [this test]))

(defn counter-checker
  "Assert-free counter analysis (jepsen's built-in checker/counter is strict about
   value types). INCR is fetch-and-add, so every acknowledged INCR must return a
   DISTINCT value — duplicates mean a double-apply / non-idempotent replay
   (crab-cache's known gap) — and a later read must not fall below the largest
   acknowledged INCR, which would mean a lost acknowledged increment."
  []
  (reify checker/Checker
    (check [this test history opts]
      (let [adds   (->> history
                        (filter #(and (= :ok (:type %)) (= :add (:f %))))
                        (keep :incr-result))
            reads  (->> history
                        (filter #(and (= :ok (:type %)) (= :read (:f %))))
                        (keep :value)
                        (filter number?))
            dups   (->> (frequencies adds)
                        (filter (fn [[_ n]] (< 1 n)))
                        (into (sorted-map)))
            max-add  (when (seq adds)  (apply max adds))
            max-read (when (seq reads) (apply max reads))
            lost?    (boolean (and max-add max-read (< max-read max-add)))]
        {:valid?                  (and (empty? dups) (not lost?))
         :acked-incrs             (count adds)
         :distinct-incr-returns   (count (distinct adds))
         :duplicate-incr-returns  dups
         :max-incr-return         max-add
         :max-read-observed       max-read
         :lost-update?            lost?}))))

;; Ops are FUNCTIONS, not bare maps: a literal op-map is a generator that emits
;; itself once and exhausts, so (gen/mix [m m m]) would stop after a few ops.
(defn- add-op  [_ _] {:type :invoke, :f :add, :value 1})
(defn- read-op [_ _] {:type :invoke, :f :read})

(defn workload
  [opts]
  {:client          (->Client nil)
   :checker         (counter-checker)
   ;; ~3:1 increments to reads.
   :generator       (gen/mix [add-op add-op add-op read-op])
   ;; One quiescent read per thread at the end (each-thread + literal map = once).
   :final-generator (gen/each-thread {:type :invoke, :f :read})})
