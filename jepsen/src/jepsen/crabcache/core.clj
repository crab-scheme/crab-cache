(ns jepsen.crabcache.core
  "Jepsen test entry point for crab-cache.

   Run from inside the jepsen/ directory on a Jepsen control node:

     lein run test --workload register --nemesis partition,kill \\
                   --nodes n1,n2,n3,n4,n5 --concurrency 10n --time-limit 120

     lein run serve            # browse results at http://localhost:8080

   See README.md for provisioning + the Docker / OrbStack node setup."
  (:require [clojure.string :as str]
            [jepsen [checker :as checker]
                    [cli :as cli]
                    [generator :as gen]
                    [tests :as tests]]
            [jepsen.nemesis :as nemesis]
            [jepsen.nemesis.combined :as nc]
            [jepsen.os :as os]
            [jepsen.crabcache [db :as db]
                              [register :as register]
                              [counter :as counter]
                              [append :as append]
                              [cas :as cas]]))

(def workloads
  "Workload name -> constructor."
  {:register register/workload
   :counter  counter/workload
   :append   append/workload
   :cas      cas/workload})

;; clock excluded: shared-kernel containers can't skew one node's clock in isolation.
(def all-faults [:partition :kill :pause])

(defn parse-faults
  [s]
  (cond (= s "none") #{}
        (= s "all")  (set all-faults)
        :else        (set (map keyword (str/split s #",")))))

(def cli-opts
  "Options on top of jepsen.cli/single-test-cmd's built-ins (which already provide
   --nodes, --concurrency, --time-limit, --test-count, --username, etc.)."
  [["-w" "--workload NAME" "Workload to run: register | counter"
    :default  :register
    :parse-fn keyword
    :validate [workloads (cli/one-of workloads)]]

   [nil "--nemesis FAULTS" "Faults: comma-separated subset of partition,kill,pause,clock (or 'none' / 'all')"
    :default  #{:partition}
    :parse-fn parse-faults
    :validate [(fn [fs] (every? (set all-faults) fs))
               (str "must be a subset of " (mapv name all-faults))]]

   [nil "--shards N" "Shards per node. 1 = single Raft group (recommended first cut)."
    :default  1
    :parse-fn #(Long/parseLong %)
    :validate [pos? "must be positive"]]

   [nil "--[no-]durable" "fsync every write (RocksDB durable mode)"
    :default true]

   [nil "--rate HZ" "Approx requests/sec/thread"
    :default  50
    :parse-fn #(Double/parseDouble %)
    :validate [pos? "must be positive"]]

   [nil "--register-group N" "register: worker threads per key (per-key concurrency; --concurrency must be a multiple)"
    :default  2
    :parse-fn #(Long/parseLong %)
    :validate [pos? "must be positive"]]

   [nil "--register-ops N" "register: ops per key — keep small so Knossos linearizability terminates"
    :default  100
    :parse-fn #(Long/parseLong %)
    :validate [pos? "must be positive"]]])

(defn crabcache-test
  "Builds a Jepsen test map from parsed CLI opts."
  [opts]
  (let [workload ((workloads (:workload opts)) opts)
        database (db/db)
        faults   (:nemesis opts)
        nopts    {:db        database
                  :nodes     (:nodes opts)
                  :faults    faults
                  :partition {:targets [:one :majority :majorities-ring]}
                  :interval  10}
        nemesis  (if (empty? faults)
                   ;; No faults: plain noop nemesis (skip all package setup!).
                   {:nemesis nemesis/noop, :generator nil, :final-generator nil, :perf #{}}
                   ;; Compose ONLY partition + db (kill/pause/start). Deliberately
                   ;; excludes file-corruption-package (wget's an x86_64 helper) and
                   ;; clock-package (needs per-node clock — impossible in shared-kernel
                   ;; containers); both would fail setup! here and we don't use them.
                   (nc/compose-packages [(nc/partition-package nopts)
                                         (nc/db-package nopts)]))]
    (merge tests/noop-test
           opts
           {:name      (str "crabcache " (name (:workload opts))
                            " s" (:shards opts)
                            (when (:durable opts) " durable")
                            " {" (str/join "," (map name (sort (:nemesis opts)))) "}")
            :os        os/noop
            :db        database
            :client    (:client workload)
            :nemesis   (:nemesis nemesis)
            :checker   (checker/compose
                         {:perf       (checker/perf {:nemeses (:perf nemesis)})
                          :clock      (checker/clock-plot)
                          :stats      (checker/stats)
                          :exceptions (checker/unhandled-exceptions)
                          :workload   (:checker workload)})
            :generator (gen/phases
                         (->> (:generator workload)
                              (gen/stagger (/ 1 (:rate opts)))
                              (gen/nemesis (:generator nemesis))
                              (gen/time-limit (:time-limit opts)))
                         (gen/log "Healing cluster")
                         (gen/nemesis (:final-generator nemesis))
                         (gen/log "Final reads")
                         (gen/clients (:final-generator workload)))})))

(defn -main
  [& args]
  (cli/run! (merge (cli/single-test-cmd {:test-fn  crabcache-test
                                         :opt-spec cli-opts})
                   (cli/serve-cmd))
            args))
