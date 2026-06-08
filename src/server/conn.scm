; server/conn.scm — the per-connection actor body (slot-routed, cluster-aware,
; pub/sub-aware).
;
; Normal mode: decode RESP, route each command to its slot's shard leader
; (local serves; remote -> -MOVED), answer stateless/CLUSTER/admin inline,
; PUBLISH/PUBSUB via the broker. On (P)SUBSCRIBE the connection switches to
; SUBSCRIBER MODE: a dedicated pusher actor becomes the sole socket writer
; (broker pushes + sub/unsub confirms all flow through it, so frames never
; interleave) while this actor keeps reading the subscriber command subset.

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
(define (local-qk cfg s) (string-append (symbol->string (cfg-my-node cfg)) ":" (number->string s)))
(define (broker-pid cfg) (table-lookup 'cc-broker (symbol->string (cfg-my-node cfg))))

(define (ask-local cfg s cmd)
  (let ((p (table-lookup 'cc-shard-pid (local-qk cfg s)))) (call p cmd)))
(define (direct-local cfg s cmd)
  (let ((p (table-lookup 'cc-shard-pid (local-qk cfg s)))) (send p (list 'direct (self) cmd)) (raw-receive)))
(define (slot-of cmd) (if (pair? (cdr cmd)) (key-slot (cadr cmd)) 0))

(define (route-to-shard cfg s cmd)
  (let ((leader (table-lookup 'cc-shard-leader (local-qk cfg s))))
    (cond
      ((not leader) (r-err "TRYAGAIN no leader for slot yet"))
      ((eqv? leader (cfg-my-node cfg)) (ask-local cfg s cmd))
      (else (let ((addr (node-addr leader (cfg-addrs cfg))))
              (if addr (r-err (string-append "MOVED " (number->string (slot-of cmd)) " " addr))
                  (r-err "TRYAGAIN leader address unknown")))))))

; perf #1/#2/#3/#4: serve GET from the in-memory map IN THIS (conn) actor — no
; shard round-trip (#3), no RocksDB + no dir+value double-read (#2/#4), raw key
; with no prefix bytevector (#1). Only when this node leads the slot AND the key
; is a live persistent string (present in cc-str). Every miss falls through to
; the shard, which reads RocksDB authoritatively and warms the map — so this is
; a pure cache: correct across recovery/failover, and TTL'd keys (never in
; cc-str) keep their lazy-expiry semantics via the shard path.
(define (get-fast cfg s operands cmd)
  (if (not (= (length operands) 1))
      (route-to-shard cfg s cmd)                 ; arity error: let the handler emit it
      (let ((leader (table-lookup 'cc-shard-leader (local-qk cfg s))))
        (cond
          ((not leader) (r-err "TRYAGAIN no leader for slot yet"))
          ((eqv? leader (cfg-my-node cfg))
           ; native lookup+encode: value bytes go straight from the table payload
           ; into the RESP bulk frame (no deep-clone, no Scheme resp-encode).
           (let ((enc (table-get-resp-bulk 'cc-str (car operands))))
             (if enc (r-raw enc) (ask-local cfg s cmd))))   ; hit: conn-local native; miss: shard (warms)
          (else (route-to-shard cfg s cmd))))))          ; remote: -> MOVED

(define (fan-out-direct cmd cfg)
  (let loop ((i 0) (acc '()))
    (if (>= i (cfg-nshards cfg)) (reverse acc) (loop (+ i 1) (cons (direct-local cfg i cmd) acc)))))

; SAVE/BGSAVE: checkpoint every local shard's RocksDB (per-node snapshot).
(define (save-all cfg)
  (let loop ((i 0) (ok #t))
    (if (>= i (cfg-nshards cfg))
        (if ok (r-ok) (r-err "ERR background save failed"))
        (let ((p (table-lookup 'cc-shard-pid (local-qk cfg i))))
          (send p (list 'checkpoint (self)))
          (loop (+ i 1) (and ok (eq? (reply-tag (raw-receive)) 'ok)))))))

; PUBLISH / PUBSUB go through the broker (synchronous ask).
(define (pubsub-numsub broker chans)
  (let loop ((cs chans) (acc '()))
    (if (null? cs)
        (r-array (reverse acc))
        (begin (send broker (list 'numsub (utf8->string (car cs)) (self)))
               (let ((n (raw-receive)))
                 (loop (cdr cs) (cons (r-int n) (cons (r-bulk (car cs)) acc))))))))

(define (pubsub-subcmd broker operands)
  (let ((sub (string-upcase (utf8->string (car operands)))))
    (cond
      ((string=? sub "CHANNELS")
       (send broker (list 'channels (self)))
       (r-array (map (lambda (c) (r-bulk (string->utf8 c))) (raw-receive))))
      ((string=? sub "NUMSUB") (pubsub-numsub broker (cdr operands)))
      (else (r-array '())))))

(define (pubsub-reply name operands cfg)
  (let ((broker (broker-pid cfg)))
    (cond
      ((string=? name "PUBLISH")
       (if (>= (length operands) 2)
           (begin (send broker (list 'publish (utf8->string (car operands)) (cadr operands) (self)))
                  (r-int (raw-receive)))
           (r-wrong-args "publish")))
      ((string=? name "PUBSUB")
       (if (null? operands) (r-wrong-args "pubsub") (pubsub-subcmd broker operands)))
      (else (r-ok)))))

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
         (operands (cdr cmd)))
    (cond
      ((or (string=? name "PUBLISH") (string=? name "PUBSUB")) (pubsub-reply name operands cfg))
      ((or (string=? name "SAVE") (string=? name "BGSAVE")) (save-all cfg))
      (else
       (let ((r (classify-route name operands (cfg-nshards cfg))))
         (cond
           ((eq? r 'crossslot) (r-err "CROSSSLOT Keys in request don't hash to the same slot"))
           ((eq? r 'cluster) (cluster-reply operands cfg))
           ((eq? r 'all) (aggregate-replies name (fan-out-direct cmd cfg)))
           ((eq? r 'any) (stateless-reply name operands))
           ((string=? name "GET") (get-fast cfg r operands cmd))   ; conn-local in-memory read
           (else (route-to-shard cfg r cmd))))))))

; ---- subscriber mode ----

(define (subscribe-cmd? cmd)
  (and (pair? cmd) (bytevector? (car cmd))
       (let ((n (string-upcase (utf8->string (car cmd)))))
         (or (string=? n "SUBSCRIBE") (string=? n "PSUBSCRIBE")))))

(define (first-sub-pos cmds)
  (let loop ((cs cmds) (i 0))
    (cond ((null? cs) #f) ((subscribe-cmd? (car cs)) i) (else (loop (cdr cs) (+ i 1))))))

(define (take-n lst n) (if (or (<= n 0) (null? lst)) '() (cons (car lst) (take-n (cdr lst) (- n 1)))))
(define (drop-n lst n) (if (or (<= n 0) (null? lst)) lst (drop-n (cdr lst) (- n 1))))

(define (confirm kind name count)
  (resp-encode (r-array (list (r-bulk (string->utf8 kind)) (r-bulk name) (r-int count)))))

; process one subscriber-mode command; returns the new subscription-name list
(define (sub-handle broker pusher cmd subs)
  (let* ((name (string-upcase (utf8->string (car cmd)))) (args (cdr cmd)))
    (cond
      ((string=? name "SUBSCRIBE")
       (let loop ((as args) (s subs))
         (if (null? as) s
             (let ((s2 (cons (utf8->string (car as)) s)))
               (send broker (list 'subscribe pusher (utf8->string (car as))))
               (send pusher (confirm "subscribe" (car as) (length s2)))
               (loop (cdr as) s2)))))
      ((string=? name "PSUBSCRIBE")
       (let loop ((as args) (s subs))
         (if (null? as) s
             (let ((s2 (cons (utf8->string (car as)) s)))
               (send broker (list 'psubscribe pusher (utf8->string (car as))))
               (send pusher (confirm "psubscribe" (car as) (length s2)))
               (loop (cdr as) s2)))))
      ((or (string=? name "UNSUBSCRIBE") (string=? name "PUNSUBSCRIBE"))
       (let ((kind (if (string=? name "UNSUBSCRIBE") "unsubscribe" "punsubscribe"))
             (tag  (if (string=? name "UNSUBSCRIBE") 'unsubscribe 'punsubscribe)))
         (let ((targets (if (null? args) (map string->utf8 subs) args)))
           (let loop ((as targets) (s subs))
             (if (null? as)
                 (begin (if (null? targets)
                            (send pusher (resp-encode (r-array (list (r-bulk (string->utf8 kind)) (r-nil) (r-int 0))))))
                        s)
                 (let ((s2 (let ((nm (utf8->string (car as))))
                             (let rm ((l s)) (cond ((null? l) '()) ((string=? (car l) nm) (cdr l)) (else (cons (car l) (rm (cdr l)))))))))
                   (send broker (list tag pusher (utf8->string (car as))))
                   (send pusher (confirm kind (car as) (length s2)))
                   (loop (cdr as) s2)))))))
      ((string=? name "PING") (send pusher (resp-encode (r-simple "PONG"))) subs)
      (else (send pusher (resp-encode (r-err (string-append "ERR Can't execute '" name "' in subscribe context")))) subs))))

(define (subscriber-loop sock cfg broker initial-cmds initial-rem)
  (let ((pusher (spawn-source "(include \"src/server/pusher.scm\")" 'pusher sock)))
    (define (run-batch cmds subs)
      (let loop ((cs cmds) (s subs)) (if (null? cs) s (loop (cdr cs) (sub-handle broker pusher (car cs) s)))))
    (let loop ((buf initial-rem) (subs (run-batch initial-cmds '())))
      (let ((chunk (tcp-recv sock RECV-MAX)))
        (if (= (bytevector-length chunk) 0)
            (begin (send broker (list 'cleanup pusher)) (send pusher 'stop))
            (let* ((data (bytevector-append buf chunk)) (parsed (resp-parse data)))
              (loop (cdr parsed) (run-batch (filter pair-cmd? (car parsed)) subs))))))))
(define (pair-cmd? c) (and (pair? c) (bytevector? (car c))))

(define (conn sock)
  (let* ((cfg (table-lookup 'cc-config "cfg"))
         (node (cfg-my-node cfg))
         (ns   (cfg-nshards cfg)))
    (let loop ((buf (make-bytevector 0 0)))
      (let ((chunk (tcp-recv sock RECV-MAX)))
        (if (= (bytevector-length chunk) 0)
            (tcp-close sock)
            (let* ((data (bytevector-append buf chunk))
                   ; perf cc-5pw.3: serve the LEADING run of locally-led GET
                   ; hits entirely in Rust (parse+lookup+frame, no Scheme
                   ; resp-parse/route/encode). `served` = framed hit replies,
                   ; `consumed` = bytes the native path handled. Everything
                   ; else (SET / non-local / miss / SUBSCRIBE / inline /
                   ; partial) flows through the existing interpreted path on
                   ; the unconsumed tail, unchanged.
                   (fg (conn-serve-gets data node ns))
                   (served (car fg)) (consumed (cdr fg))
                   (dlen (bytevector-length data)))
              (if (> (bytevector-length served) 0) (tcp-send sock served))
              (if (= consumed dlen)
                  (loop (make-bytevector 0 0))
                  (let* ((data (subbv data consumed dlen))
                         (parsed (resp-parse data))
                         (cmds (car parsed)) (rem (cdr parsed)) (sp (first-sub-pos cmds)))
                    (if sp
                        (let ((pre (take-n cmds sp)) (subc (drop-n cmds sp)))
                          (let ((r (serve-commands pre cfg)))
                            (if (> (bytevector-length (car r)) 0) (tcp-send sock (car r))))
                          (subscriber-loop sock cfg (broker-pid cfg) subc rem))
                        (let ((result (serve-commands cmds cfg)))
                          (if (> (bytevector-length (car result)) 0) (tcp-send sock (car result)))
                          (if (cdr result) (loop rem) (tcp-close sock))))))))))))

(define (serve-commands cmds cfg)
  (let loop ((cs cmds) (out (make-bytevector 0 0)))
    (if (null? cs)
        (cons out #t)
        (let ((cmd (car cs)))
          (if (and (pair? cmd) (eq? (car cmd) 'protocol-error))
              (cons (bytevector-append out (resp-encode (r-err (cadr cmd)))) #f)
              (loop (cdr cs) (bytevector-append out (resp-encode (route-reply cmd cfg)))))))))
