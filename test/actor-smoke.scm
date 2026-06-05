; actor-smoke.scm — prove the Phase-4 spine: a shard-owner actor (its own
; thread, fresh runtime) loads the whole cache, owns the RocksDB handle
; (crossed as a sendable fixnum), and answers SET/GET sent by a client
; actor. The non-actor main observes the result via a process-global table.
;
;   crabscheme run test/actor-smoke.scm   (from repo root)

(define H (store-open "/tmp/cc-actor-smoke-db"))
(make-table 'cc "set")

; The shard-owner: a fresh runtime that includes the cache and serves
; commands. `handle` (a fixnum) indexes the process-global store registry,
; so it's valid here even though this runs in a different runtime.
(define owner-src "
  (include \"src/reply.scm\")
  (include \"src/encoding.scm\")
  (include \"src/store-ctx.scm\")
  (include \"src/shard.scm\")
  (include \"src/commands/string.scm\")
  (include \"src/commands/keys.scm\")
  (define (owner handle)
    (let ((ctx (make-ctx handle \"default\")))
      (let loop ()
        (let ((m (raw-receive)))
          (if (pair? m) (send (car m) (shard-dispatch ctx (cdr m))))
          (loop)))))")

(define owner (spawn-source owner-src 'owner H))
(table-insert! 'cc "owner" owner)

; The client: SET k v, then GET k, stash the GET reply payload for main.
(define client-src "
  (define (b s) (string->utf8 s))
  (define (client)
    (let ((o (table-lookup 'cc \"owner\")))
      (send o (cons (self) (list (b \"SET\") (b \"k\") (b \"hello-actor\"))))
      (raw-receive)
      (send o (cons (self) (list (b \"GET\") (b \"k\"))))
      (let ((r (raw-receive)))
        ; r is a reply (bulk . bytevector); stash the bytes
        (table-insert! 'cc \"got\" (cdr r)))))")
(spawn-source client-src 'client)

(define (await k)
  (let loop ((i 0))
    (let ((a (table-lookup 'cc k)))
      (cond (a a)
            ((> i 20000000) (error "actor-smoke: timeout"))
            (else (loop (+ i 1)))))))

(define got (await "got"))
(store-close H)
(display "GET via shard-owner actor => ") (display (utf8->string got)) (newline)
(if (string=? (utf8->string got) "hello-actor")
    (begin (display "actor spine OK") (newline))
    (error "actor-smoke: wrong value" got))
