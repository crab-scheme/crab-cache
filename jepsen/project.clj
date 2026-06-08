(defproject jepsen.crabcache "0.1.0-SNAPSHOT"
  :description "Jepsen tests for crab-cache (a Redis-compatible, Raft-replicated cache written in CrabScheme)"
  :url "https://github.com/crab-scheme/crab-cache"
  :license {:name "MIT"}
  :main jepsen.crabcache.core
  :jvm-opts ["-Xmx4g"
             "-Djava.awt.headless=true"
             ;; Knossos / Elle want a deep stack on big histories.
             "-server"]
  :dependencies [[org.clojure/clojure "1.11.4"]
                 [jepsen "0.3.11"]
                 [com.taoensso/carmine "3.5.0"]]
  :repl-options {:init-ns jepsen.crabcache.core})
