; resp.scm — RESP2 wire codec over bytevectors (design §9, DD-2).
;
; This is protocol logic, hence part of the cache, hence CrabScheme (§0
; mandate). The decoder is pipelining- and partial-frame-aware: a client
; connection accumulates bytes and hands the whole buffer here; we return
; every COMPLETE command plus the unconsumed tail (the start of a frame
; that hasn't fully arrived), which the caller prepends to the next read.
;
; Request frames (what clients send):
;   - RESP array of bulk strings:  *N\r\n  ($len\r\n<bytes>\r\n) x N
;   - inline:  a bare CRLF/LF-terminated line, space-split (telnet/nc).
; Reply frames (what we send) are produced by resp-encode from a reply.scm
; value: simple (+), error (-), integer (:), bulk ($), array (*), nulls.
;
; Depends on: reply.scm.

(define CR 13)
(define LF 10)

; Find the index of the CR in the next CRLF at/after i, or #f. (RESP framing
; lines always end CRLF.)
(define (scan-crlf buf i n)
  (let loop ((j i))
    (cond ((>= (+ j 1) n) #f)
          ((and (= (bytevector-u8-ref buf j) CR)
                (= (bytevector-u8-ref buf (+ j 1)) LF)) j)
          (else (loop (+ j 1))))))

; Parse a base-10 integer (optional leading '-') from buf[start,end).
(define (range->int buf start end)
  (let loop ((j start) (neg #f) (acc 0) (any #f))
    (if (>= j end)
        (if any (if neg (- acc) acc) #f)
        (let ((c (bytevector-u8-ref buf j)))
          (cond ((and (= c 45) (= j start)) (loop (+ j 1) #t acc any))   ; '-'
                ((and (>= c 48) (<= c 57))
                 (loop (+ j 1) neg (+ (* acc 10) (- c 48)) #t))
                (else #f))))))                                            ; junk

; ---- request decoding ----
;
; (resp-parse buf) -> (cons commands remainder)
;   commands  : list of commands, each a list of bytevector arguments
;   remainder : bytevector of trailing bytes belonging to an incomplete frame
; On a protocol error the command list element is (list 'protocol-error msg)
; — callers reply -ERR and close.

; Parse ONE frame at offset i. Returns:
;   (vector 'ok args next-i) | (vector 'incomplete) | (vector 'error msg)
(define (parse-frame buf i n)
  (if (>= i n)
      (vector 'incomplete)
      (if (= (bytevector-u8-ref buf i) 42)        ; '*'
          (parse-array buf i n)
          (parse-inline buf i n))))

(define (parse-array buf i n)
  (let ((hdr (scan-crlf buf i n)))
    (if (not hdr)
        (vector 'incomplete)
        (let ((count (range->int buf (+ i 1) hdr)))
          (if (or (not count) (< count 0))
              (vector 'error "ERR Protocol error: invalid multibulk length")
              (let loop ((k 0) (pos (+ hdr 2)) (acc '()))
                (if (= k count)
                    (vector 'ok (reverse acc) pos)
                    (let ((bulk (parse-bulk buf pos n)))
                      (cond ((eq? (vector-ref bulk 0) 'ok)
                             (loop (+ k 1) (vector-ref bulk 2)
                                   (cons (vector-ref bulk 1) acc)))
                        (else bulk))))))))))            ; incomplete or error bubbles up

(define (parse-bulk buf i n)
  (if (or (>= i n) (not (= (bytevector-u8-ref buf i) 36)))   ; '$'
      (if (>= i n) (vector 'incomplete)
          (vector 'error "ERR Protocol error: expected '$'"))
      (let ((hdr (scan-crlf buf i n)))
        (if (not hdr)
            (vector 'incomplete)
            (let ((len (range->int buf (+ i 1) hdr)))
              (if (or (not len) (< len 0))
                  (vector 'error "ERR Protocol error: invalid bulk length")
                  (let ((data-start (+ hdr 2)))
                    (if (> (+ data-start len 2) n)          ; need data + CRLF
                        (vector 'incomplete)
                        (vector 'ok
                                (subbv buf data-start (+ data-start len))
                                (+ data-start len 2))))))))))

; Inline command: read a CRLF- or LF-terminated line, split on spaces/tabs.
(define (parse-inline buf i n)
  (let loop ((j i))
    (cond ((>= j n) (vector 'incomplete))
          ((= (bytevector-u8-ref buf j) LF)
           (let ((end (if (and (> j i) (= (bytevector-u8-ref buf (- j 1)) CR)) (- j 1) j)))
             (vector 'ok (split-ws buf i end) (+ j 1))))
          (else (loop (+ j 1))))))

(define (split-ws buf start end)
  (let loop ((j start) (tok-start #f) (acc '()))
    (cond
      ((>= j end)
       (reverse (if tok-start (cons (subbv buf tok-start end) acc) acc)))
      (else
       (let ((c (bytevector-u8-ref buf j)))
         (if (or (= c 32) (= c 9))                  ; space / tab
             (loop (+ j 1) #f (if tok-start (cons (subbv buf tok-start j) acc) acc))
             (loop (+ j 1) (if tok-start tok-start j) acc)))))))

(define (resp-parse buf)
  (let ((n (bytevector-length buf)))
    (let loop ((i 0) (cmds '()))
      (if (>= i n)
          (cons (reverse cmds) (make-bytevector 0 0))
          (let ((f (parse-frame buf i n)))
            (case (vector-ref f 0)
              ((ok)
               ; skip empty inline lines (e.g. a stray CRLF)
               (if (null? (vector-ref f 1))
                   (loop (vector-ref f 2) cmds)
                   (loop (vector-ref f 2) (cons (vector-ref f 1) cmds))))
              ((incomplete)
               (cons (reverse cmds) (subbv buf i n)))
              ((error)
               (cons (reverse (cons (list 'protocol-error (vector-ref f 1)) cmds))
                     (make-bytevector 0 0)))))))))

; ---- reply encoding ----

(define (byte b) (let ((v (make-bytevector 1 b))) v))
(define crlf-bv (string->utf8 "\r\n"))
(define (num->bv n) (string->utf8 (number->string n)))

(define (resp-encode r)
  (case (reply-tag r)
    ((ok)        (string->utf8 "+OK\r\n"))
    ((simple)    (bytevector-append (string->utf8 "+") (string->utf8 (reply-val r)) crlf-bv))
    ((err)       (bytevector-append (string->utf8 "-") (string->utf8 (reply-val r)) crlf-bv))
    ((int)       (bytevector-append (string->utf8 ":") (num->bv (reply-val r)) crlf-bv))
    ((bulk)      (let ((bv (reply-val r)))
                   (bytevector-append (string->utf8 "$")
                                      (num->bv (bytevector-length bv)) crlf-bv
                                      bv crlf-bv)))
    ((nil)       (string->utf8 "$-1\r\n"))
    ((nil-array) (string->utf8 "*-1\r\n"))
    ((array)     (let ((elems (reply-val r)))
                   (let loop ((e elems) (acc (bytevector-append
                                              (string->utf8 "*") (num->bv (length elems)) crlf-bv)))
                     (if (null? e)
                         acc
                         (loop (cdr e) (bytevector-append acc (resp-encode (car e))))))))
    (else (string->utf8 "-ERR internal: bad reply\r\n"))))
