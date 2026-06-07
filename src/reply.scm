; reply.scm — the RESP-agnostic reply ADT.
;
; Command handlers return *replies* in this neutral algebra; the RESP2
; codec (Phase 4, resp.scm) is the only thing that turns them into wire
; bytes. Keeping commands ignorant of RESP is what lets the same command
; layer later answer a non-RESP front-end if we ever want one, and keeps
; the §0 protocol logic (encoding) cleanly separated from semantics.
;
; A reply is a tagged pair  (tag . payload):
;
;   (ok)                 -> +OK
;   (simple . "PONG")    -> +<string>            (status; payload is a string)
;   (err . "ERR msg")    -> -<string>            (payload INCLUDES the code word)
;   (int . 42)           -> :<integer>
;   (bulk . <bytevector>)-> $<len>\r\n<bytes>     (binary-safe bulk string)
;   (nil)                -> $-1                    (null bulk)
;   (array . <list>)     -> *<n> then each element reply
;   (nil-array)          -> *-1                    (null array)
;
; Payloads are bytevectors for bulk (binary-safe), strings for simple/err,
; exact integers for int, and lists-of-replies for array.

(define (r-ok)          (cons 'ok #f))
(define (r-simple s)    (cons 'simple s))           ; s : string
(define (r-err msg)     (cons 'err msg))            ; msg : string incl. code, e.g. "ERR ..."
(define (r-int n)       (cons 'int n))              ; n : exact integer
(define (r-bulk bv)     (cons 'bulk bv))            ; bv : bytevector
(define (r-nil)         (cons 'nil #f))
(define (r-array lst)   (cons 'array lst))          ; lst : list of replies
(define (r-nil-array)   (cons 'nil-array #f))
(define (r-raw bytes)   (cons 'raw bytes))          ; bytes : already-RESP-encoded bytevector (passthrough)

; Convenience: a bulk reply from a Scheme string.
(define (r-str s) (r-bulk (string->utf8 s)))

(define (reply-tag r) (car r))
(define (reply-val r) (cdr r))

; Common typed-error replies (Redis wording).
(define (r-wrongtype)
  (r-err "WRONGTYPE Operation against a key holding the wrong kind of value"))
(define (r-not-int)
  (r-err "ERR value is not an integer or out of range"))
(define (r-not-float)
  (r-err "ERR value is not a valid float"))
(define (r-syntax)
  (r-err "ERR syntax error"))
(define (r-wrong-args name)
  (r-err (string-append "ERR wrong number of arguments for '" name "' command")))
