; shard.scm — the command registry, the dispatcher, and the shard-owner
; actor.
;
; The shard-owner is the actor that *owns* a shard's store handle and
; serializes that shard's commands — the Redis-semantic logic. In Phase 3
; it applies directly to the local RocksDB state machine (no Raft yet;
; Phase 5 routes proposals through a per-shard Raft group). Because one
; actor owns the shard, commands on it are linearized for free — the same
; single-writer discipline Redis gets from being single-threaded per shard.
;
; Depends on: reply.scm. Command modules register into the table here.

; command-name symbol -> handler  (names are interned uppercased symbols)
(define *commands* (make-eqv-hashtable))

(define (register-command! name handler)
  (hashtable-set! *commands* (string->symbol name) handler))

(define (cmd-name-symbol bv) (string->symbol (string-upcase (utf8->string bv))))

; Dispatch a decoded command (a non-empty list of bytevector arguments,
; arg0 = command name) against ctx, returning a reply.
(define (shard-dispatch ctx cmd)
  (if (null? cmd)
      (r-err "ERR empty command")
      (let ((h (hashtable-ref *commands* (cmd-name-symbol (car cmd)) #f)))
        (if h
            (h ctx (cdr cmd))
            (r-err (string-append
                    "ERR unknown command '" (utf8->string (car cmd)) "'"))))))

; ---- shard-owner actor ----
;
; Protocol matches beam's (call pid msg): the caller sends (sender . msg)
; and we reply with (send sender result). So a conn-actor does
; (call owner cmd-list) and gets back a reply.

(define (spawn-shard-owner ctx)
  (spawn-activation (lambda () (shard-owner-loop ctx))))

; raw-receive returns the sent payload directly; (call …) delivers
; (sender . cmd-list). Reply to the sender with the dispatched result.
(define (shard-owner-loop ctx)
  (let ((m (raw-receive #f)))
    (if (pair? m)
        (send (car m) (shard-dispatch ctx (cdr m))))
    (shard-owner-loop ctx)))
