; server/conn.scm — the per-connection actor body (slot-routed, cluster-aware).
;
; One spawn-source actor (own OS thread) per accepted socket. It decodes RESP
; and routes each command:
;   - keyed command -> its slot's shard. If THIS node leads that shard, ask the
;     local replica (writes propose through Raft; reads served by the leader).
;     If another node leads it, reply -MOVED slot host:port (the client
;     re-dials the right node — standard Redis Cluster behavior).
;   - PING/ECHO/SELECT/COMMAND/INFO/QUIT -> answered statelessly here.
;   - CLUSTER -> rendered from topology.
;   - DBSIZE/FLUSHALL/KEYS -> per-node: fan out 'direct to local replicas + aggregate.
;   - multi-key spanning slots -> -CROSSSLOT.
;
; cfg = (host port nshards node-id ranges my-node addrs), addrs = list of
; (node-name . "host:port") for MOVED. In Phase 5 (single node) every shard's
; leader is this node, so nothing is ever MOVED.

(include "src/reply.scm")
(include "src/encoding.scm")
(include "src/resp.scm")
(include "src/slotmap.scm")
(include "src/router.scm")
(include "src/commands/cluster.scm")

(define RECV-MAX 65536)

(define (cfg-my-node cfg) (list-ref cfg 5))
(define (cfg-addrs cfg)   (list-ref cfg 6))
(define (node-addr nd addrs) (let ((p (assq nd addrs))) (and p (cdr p))))
(define (local-qk cfg s)
  (string-append (symbol->string (cfg-my-node cfg)) ":" (number->string s)))

(define (ask-local cfg s cmd)
  (let ((p (table-lookup 'cc-shard-pid (local-qk cfg s))))
    (send p (cons (self) cmd))
    (raw-receive)))

(define (direct-local cfg s cmd)
  (let ((p (table-lookup 'cc-shard-pid (local-qk cfg s))))
    (send p (list 'direct (self) cmd))
    (raw-receive)))

(define (slot-of cmd)
  (if (pair? (cdr cmd)) (key-slot (cadr cmd)) 0))

; route a keyed command to shard s: local leader serves, remote leader -> MOVED
(define (route-to-shard cfg s cmd)
  (let ((leader (table-lookup 'cc-shard-leader (local-qk cfg s))))
    (cond
      ((not leader) (r-err "TRYAGAIN no leader for slot yet"))
      ((eqv? leader (cfg-my-node cfg)) (ask-local cfg s cmd))
      (else
       (let ((addr (node-addr leader (cfg-addrs cfg))))
         (if addr
             (r-err (string-append "MOVED " (number->string (slot-of cmd)) " " addr))
             (r-err "TRYAGAIN leader address unknown")))))))

(define (fan-out-direct cmd cfg)
  (let loop ((i 0) (acc '()))
    (if (>= i (cfg-nshards cfg)) (reverse acc)
        (loop (+ i 1) (cons (direct-local cfg i cmd) acc)))))

(define (stateless-reply name operands)
  (cond
    ((string=? name "PING") (if (null? operands) (r-simple "PONG") (r-bulk (car operands))))
    ((string=? name "ECHO") (if (pair? operands) (r-bulk (car operands)) (r-wrong-args "echo")))
    ((string=? name "SELECT") (r-ok))
    ((string=? name "COMMAND") (r-array '()))
    ((string=? name "QUIT") (r-ok))
    ((string=? name "INFO")
     (r-bulk (string->utf8 "# Server\r\nredis_version:7.4.0-crabscheme\r\ncrab_cache:1\r\n")))
    (else (r-ok))))

(define (route-reply cmd cfg)
  (let* ((name (string-upcase (utf8->string (car cmd))))
         (operands (cdr cmd))
         (r (classify-route name operands (cfg-nshards cfg))))
    (cond
      ((eq? r 'crossslot) (r-err "CROSSSLOT Keys in request don't hash to the same slot"))
      ((eq? r 'cluster) (cluster-reply operands cfg))
      ((eq? r 'all) (aggregate-replies name (fan-out-direct cmd cfg)))
      ((eq? r 'any) (stateless-reply name operands))
      (else (route-to-shard cfg r cmd)))))

(define (conn sock)
  (let ((cfg (table-lookup 'cc-config "cfg")))
    (let loop ((buf (make-bytevector 0 0)))
      (let ((chunk (tcp-recv sock RECV-MAX)))
        (if (= (bytevector-length chunk) 0)
            (tcp-close sock)
            (let* ((data   (bytevector-append buf chunk))
                   (parsed (resp-parse data))
                   (cmds   (car parsed))
                   (rem    (cdr parsed))
                   (result (serve-commands cmds cfg)))
              (if (> (bytevector-length (car result)) 0)
                  (tcp-send sock (car result)))
              (if (cdr result) (loop rem) (tcp-close sock))))))))

(define (serve-commands cmds cfg)
  (let loop ((cs cmds) (out (make-bytevector 0 0)))
    (if (null? cs)
        (cons out #t)
        (let ((cmd (car cs)))
          (if (and (pair? cmd) (eq? (car cmd) 'protocol-error))
              (cons (bytevector-append out (resp-encode (r-err (cadr cmd)))) #f)
              (loop (cdr cs)
                    (bytevector-append out (resp-encode (route-reply cmd cfg)))))))))
