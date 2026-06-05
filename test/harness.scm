; test/harness.scm — check infrastructure + command-invocation helpers.
;
; Tests drive the cache the way the RESP front-end will: build a command
; as a list of bytevector arguments and dispatch it against a shard-ctx,
; then assert on the reply (decoded to a friendly s-expr).

(define *checks* 0)
(define *fails* 0)

(define (check name expected actual)
  (set! *checks* (+ *checks* 1))
  (if (equal? expected actual)
      (begin (display "  ok   ") (display name) (newline))
      (begin (set! *fails* (+ *fails* 1))
             (display "  FAIL ") (display name)
             (display "  expected=") (write expected)
             (display "  got=") (write actual) (newline))))

(define (section name)
  (display "== ") (display name) (display " ==") (newline))

(define (done!)
  (newline)
  (display *checks*) (display " checks, ")
  (display *fails*) (display " failed") (newline)
  (if (> *fails* 0)
      (error "TESTS FAILED" *fails*)
      (begin (display "ALL PASS") (newline))))

; coerce strings/symbols/numbers to bytevector arguments
(define (->bv x)
  (cond ((bytevector? x) x)
        ((string? x) (string->utf8 x))
        ((symbol? x) (string->utf8 (symbol->string x)))
        ((number? x) (string->utf8 (number->string x)))
        (else (error "->bv: cannot coerce" x))))

; run a command:  (rc ctx "SET" "k" "v")  ->  reply
(define (rc ctx . parts) (shard-dispatch ctx (map ->bv parts)))

; decode a reply to a comparable s-expr (bulk strings shown as Scheme
; strings under the assumption they're UTF-8 — fine for tests).
(define (reply->sexp r)
  (case (reply-tag r)
    ((ok) 'OK)
    ((simple) (reply-val r))
    ((err) (list 'err (reply-val r)))
    ((int) (reply-val r))
    ((bulk) (utf8->string (reply-val r)))
    ((nil) 'nil)
    ((nil-array) 'nil-array)
    ((array) (map reply->sexp (reply-val r)))
    (else (list 'unknown-reply r))))

; convenience: dispatch + decode in one step
(define (rcx ctx . parts) (reply->sexp (apply rc ctx parts)))

; assert a command's decoded reply equals `expected`
(define (check-cmd name expected ctx . parts)
  (check name expected (reply->sexp (apply rc ctx parts))))
