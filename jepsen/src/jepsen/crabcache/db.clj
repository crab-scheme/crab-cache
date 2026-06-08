(ns jepsen.crabcache.db
  "Install / start / stop / kill / pause a crab-cache CLUSTER node on each
   Jepsen DB node.

   crab-cache is interpreted, so a node is `crabscheme run src/node-cluster.scm`
   rather than a compiled service. We do NOT build it here: the crabscheme
   binary (built with --features stdlib-store) and the crab-cache `src/` tree are
   expected under `/opt/crabcache` on every node — run `bin/sync-nodes.sh` once
   before the first test. setup! then just starts the process; kill!/pause! drive
   the nemesis; teardown! wipes data + logs but leaves the binary in place."
  (:require [clojure.string :as str]
            [clojure.tools.logging :refer [info]]
            [jepsen [control :as c]
                    [db :as db]]
            [jepsen.control.util :as cu]))

(def dir          "/opt/crabcache")
(def binary       (str dir "/crabscheme"))
(def logfile      (str dir "/node.log"))
(def pidfile      (str dir "/node.pid"))
(def data-dir     (str dir "/data"))
(def raft-port    7000)
(def client-port  6379)
(def proc-pattern "node-cluster.scm")   ; matches the running process cmdline

(defn cluster-spec
  "The --cluster argument: name:host:raftport:clientport,... for every node.
   We use the Jepsen node name as both the crab-cache node name and its host,
   since Jepsen's network resolves node names (n1, n2, ...)."
  [test]
  (->> (:nodes test)
       (map (fn [n] (str (name n) ":" (name n) ":" raft-port ":" client-port)))
       (str/join ",")))

(defn start-node!
  "Launch the node-cluster process under start-stop-daemon. chdir to `dir` so the
   script's relative (include \"src/...\") forms resolve."
  [test node]
  (cu/start-daemon!
    {:logfile logfile
     :pidfile pidfile
     :chdir   dir}
    binary
    "run" "src/node-cluster.scm" "--"
    "--node"    (name node)
    "--shards"  (str (:shards test))
    "--durable" (if (:durable test) "yes" "no")
    ;; shards land at <data-dir>/cc-shard0, -shard1, ... — all UNDER data-dir so
    ;; wiping data-dir on setup! actually clears every shard's RocksDB.
    "--db"      (str data-dir "/cc")
    "--cluster" (cluster-spec test)))

(defn signal!
  "Send a signal to the node process by cmdline match. Tolerates 'no process'."
  [sig]
  (c/su (try (c/exec :pkill sig :-f proc-pattern)
             (catch Exception _ :not-running))))

(defn db
  "crab-cache cluster DB."
  []
  (reify
    db/DB
    (setup! [this test node]
      (when-not (cu/exists? binary)
        (throw (ex-info (str "crabscheme binary not found at " binary
                             " — run jepsen/bin/sync-nodes.sh to provision the nodes first")
                        {:node node :expected binary})))
      ;; Defensive: kill any stale crabscheme from a prior run whose teardown
      ;; didn't fire. crab-cache's Raft busy-polls, so orphaned processes peg the
      ;; CPU and both slow sshd and skew results. Then clear RocksDB and start.
      (signal! :-9)
      (Thread/sleep 500)
      (c/su (c/exec :rm :-rf data-dir)
            (c/exec :rm :-f logfile pidfile)
            (c/exec :mkdir :-p data-dir))
      (info node "starting crab-cache")
      (start-node! test node)
      ;; Give the mesh + per-shard leader election time to settle before clients
      ;; connect (the node only opens its client port once a leader is known).
      (Thread/sleep 8000))

    (teardown! [this test node]
      (info node "stopping crab-cache")
      (signal! :-9)
      (c/su (c/exec :rm :-rf data-dir)
            (c/exec :rm :-f logfile pidfile)))

    db/LogFiles
    (log-files [this test node] [logfile])

    ;; --- nemesis hooks ---
    db/Kill
    (start! [this test node]
      ;; Restart WITHOUT wiping data — this exercises RocksDB crash recovery
      ;; + Raft rejoin, exactly what we want to verify.
      (when-not (cu/daemon-running? pidfile)
        (start-node! test node))
      :started)
    (kill! [this test node]
      (signal! :-9)
      :killed)

    db/Pause
    (pause!  [this test node] (signal! :-STOP) :paused)
    (resume! [this test node] (signal! :-CONT) :resumed)))
