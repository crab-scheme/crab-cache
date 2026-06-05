; commands/server.scm — connection/server/admin commands.
;
; PING ECHO SELECT INFO DBSIZE FLUSHALL FLUSHDB COMMAND QUIT
;
; Depends on: encoding.scm, store-ctx.scm, reply.scm, shard.scm.

(define (cmd-ping ctx operands)
  (cond ((null? operands) (r-simple "PONG"))
        ((null? (cdr operands)) (r-bulk (car operands)))
        (else (r-wrong-args "ping"))))

(define (cmd-echo ctx operands)
  (if (= (length operands) 1) (r-bulk (car operands)) (r-wrong-args "echo")))

; single logical DB (0). SELECT 0 -> OK; anything else -> error.
(define (cmd-select ctx operands)
  (if (= (length operands) 1)
      (let ((n (bytes->int (car operands))))
        (cond ((not n) (r-not-int))
              ((= n 0) (r-ok))
              (else (r-err "ERR DB index is out of range"))))
      (r-wrong-args "select")))

; all live user keys (directory scan, lazy-expiry-aware)
(define (all-live-ukeys ctx)
  (let loop ((rows (kv-scan ctx TAG-DIR)) (acc '()))
    (if (null? rows)
        (reverse acc)
        (let* ((fk (caar rows))
               (uk (subbv fk (bytevector-length TAG-DIR) (bytevector-length fk))))
          (loop (cdr rows) (if (key-exists? ctx uk) (cons uk acc) acc))))))

(define (cmd-dbsize ctx operands)
  (r-int (length (all-live-ukeys ctx))))

(define (flush ctx)
  (for-each (lambda (uk) (purge-key! ctx uk)) (all-live-ukeys ctx))
  (r-ok))
(define (cmd-flushall ctx operands) (flush ctx))   ; ignores ASYNC/SYNC arg
(define (cmd-flushdb ctx operands) (flush ctx))

; Minimal INFO — enough for clients that probe it; not a full mirror.
(define (cmd-info ctx operands)
  (r-bulk (string->utf8
           (string-append
            "# Server\r\n"
            "redis_version:7.4.0-crabscheme\r\n"
            "crab_cache:1\r\n"
            "# Keyspace\r\n"
            "db0:keys=" (number->string (length (all-live-ukeys ctx))) "\r\n"))))

; redis-cli probes COMMAND DOCS / COMMAND COUNT on connect; answer benignly.
(define (cmd-command ctx operands)
  (if (and (pair? operands)
           (string=? (string-upcase (bv->s (car operands))) "COUNT"))
      (r-int (hashtable-size *commands*))
      (r-array '())))

(define (cmd-quit ctx operands) (r-ok))

(register-command! "PING" cmd-ping)
(register-command! "ECHO" cmd-echo)
(register-command! "SELECT" cmd-select)
(register-command! "DBSIZE" cmd-dbsize)
(register-command! "FLUSHALL" cmd-flushall)
(register-command! "FLUSHDB" cmd-flushdb)
(register-command! "INFO" cmd-info)
(register-command! "COMMAND" cmd-command)
(register-command! "QUIT" cmd-quit)
