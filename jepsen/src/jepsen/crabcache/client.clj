(ns jepsen.crabcache.client
  "Carmine-based RESP client with MOVED-redirect following.

   crab-cache shards keys across per-shard Raft groups and replies `-MOVED <slot>
   <host:port>` for any keyed command this node doesn't currently lead. Even with
   --shards 1 (a single Raft group), a *follower* leads nothing and thus MOVEDs
   every keyed command to the current leader — so the client MUST follow
   redirects. This is also exactly the failover-recovery path we want to exercise:
   when the leader dies and a survivor is elected, in-flight clients get MOVED to
   the new leader and re-aim there.

   Carmine is not cluster-aware, so we implement the redirect ourselves: each
   logical client holds an atom of its current target host; `exec-cmd` retries
   against the MOVED target (bounded)."
  (:require [clojure.string :as str]
            [taoensso.carmine :as car]))

(def client-port 6379)

(defn conn-spec
  "A Carmine connection-options map aimed at `host`."
  [host]
  {:spec {:host host :port client-port :timeout-ms 10000}})

(defn classify-error
  "Inspect an exception or Throwable reply for a crab-cache control signal:
   returns [:moved host] for a MOVED redirect, [:tryagain] for a transient
   TRYAGAIN / 'no leader for slot yet' (the op was REJECTED, not applied — safe
   to retry without ambiguity), or nil for anything else (a real error)."
  [x]
  (let [s (cond (instance? Throwable x) (str (.getMessage ^Throwable x) " " x)
                :else                   (str x))]
    (cond
      (re-find #"MOVED\s+\d+\s+(\S+):\d+" s)
      [:moved (nth (re-find #"MOVED\s+\d+\s+(\S+):\d+" s) 1)]

      (re-find #"(?i)TRYAGAIN|no leader" s)
      [:tryagain])))

(defn exec-cmd
  "Run (f conn-spec) against the client's current target. Follows MOVED redirects
   (re-aiming `node-atom`) and retries transient TRYAGAIN/no-leader rejections
   with a short backoff — both bounded. On exhaustion or any other error the
   throwable propagates (the jepsen client turns it into an :info op, which is
   correct: only definitely-not-applied rejections are retried here). Carmine may
   throw a Redis error or return it as a Throwable reply, so we handle both."
  [node-atom f]
  (loop [redirects 0, retries 0]
    (let [r (try
              (let [res (f (conn-spec @node-atom))]
                (if (instance? Throwable res)
                  (or (classify-error res) (throw res))
                  [:ok res]))
              (catch Throwable e
                (or (classify-error e) (throw e))))]
      (case (nth r 0)
        :ok       (nth r 1)
        :moved    (if (< redirects 15)
                    (do (reset! node-atom (nth r 1))
                        (recur (inc redirects) retries))
                    (throw (ex-info "too many MOVED redirects" {:redirects redirects})))
        :tryagain (if (< retries 50)
                    (do (Thread/sleep 100)
                        (recur redirects (inc retries)))
                    (throw (ex-info "no leader after TRYAGAIN retries" {:retries retries})))))))
