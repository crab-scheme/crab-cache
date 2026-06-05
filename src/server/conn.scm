; server/conn.scm — the per-connection actor body (Phase 5: slot-routed).
;
; One spawn-source actor (own OS thread) per accepted socket. It decodes RESP,
; routes each command by CRC16 keyslot to the owning shard's replica actor
; (via the process-global shard-pid table), forwards it, and writes the
; batched reply. Keyless/admin commands go to shard 0; DBSIZE/FLUSHALL/KEYS
; fan out to every shard and aggregate; CLUSTER is answered from topology;
; multi-key commands spanning slots get -CROSSSLOT.
;
; Loaded by spawn-source: (spawn-source "(include \"src/server/conn.scm\")" 'conn sock)

(include "src/reply.scm")
(include "src/encoding.scm")
(include "src/resp.scm")
(include "src/slotmap.scm")
(include "src/router.scm")
(include "src/commands/cluster.scm")

(define RECV-MAX 65536)

(define (shard-pid i) (table-lookup 'cc-shard-pid (number->string i)))

; synchronous RPC to a shard replica: send (self . cmd), block for the reply.
(define (ask-shard i cmd)
  (let ((p (shard-pid i)))
    (send p (cons (self) cmd))
    (raw-receive)))

(define (fan-out-all cmd nshards)
  (let loop ((i 0) (acc '()))
    (if (>= i nshards) (reverse acc) (loop (+ i 1) (cons (ask-shard i cmd) acc)))))

; route one decoded command to a reply
(define (route-reply cmd cfg)
  (let* ((name (string-upcase (utf8->string (car cmd))))  ; clients send any case
         (operands (cdr cmd))
         (nshards (cfg-nshards cfg))
         (r (classify-route name operands nshards)))
    (cond
      ((eq? r 'crossslot)
       (r-err "CROSSSLOT Keys in request don't hash to the same slot"))
      ((eq? r 'cluster) (cluster-reply operands cfg))
      ((eq? r 'all) (aggregate-replies name (fan-out-all cmd nshards)))
      ((eq? r 'any) (ask-shard 0 cmd))
      (else (ask-shard r cmd)))))

(define (conn sock)
  (let ((cfg (table-lookup 'cc-config "cfg")))     ; (host port nshards id ranges)
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

; (out-bytevector . keep-open?)
(define (serve-commands cmds cfg)
  (let loop ((cs cmds) (out (make-bytevector 0 0)))
    (if (null? cs)
        (cons out #t)
        (let ((cmd (car cs)))
          (if (and (pair? cmd) (eq? (car cmd) 'protocol-error))
              (cons (bytevector-append out (resp-encode (r-err (cadr cmd)))) #f)
              (loop (cdr cs)
                    (bytevector-append out (resp-encode (route-reply cmd cfg)))))))))
