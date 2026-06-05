; encoding.scm — the RocksDB key/value byte schema (design §7).
;
; Pure byte functions only: NO store access lives here. Everything is a
; bytevector in/out so keys and values are binary-safe.
;
; KEYSPACE DIRECTORY (the authoritative "does key K exist / what type / TTL")
;   T:<ukey>  ->  [type:1][deadline:u64-be]
;     type byte is an ASCII char:  s string  h hash  l list  e set  z zset
;     deadline 0 = no expiry; else a *logical-clock* tick deadline (§7,
;     DD-3): replica-deterministic, compared against the shard clock on read.
;   One directory get per command yields existence + type + ttl, makes TYPE
;   and WRONGTYPE checks O(1), and is the single source of truth for a key's
;   identity. Per-type data lives under the prefixes below.
;
; PER-TYPE DATA  (composite keys are length-prefixed: tag ++ u32be(len ukey)
; ++ ukey ++ suffix — binary-safe, and a clean prefix for "all of key K".
; This is a deliberate, encoding-internal refinement of §7's \0 separators
; so arbitrary binary keys/members can't collide on a separator byte.)
;
;   string   S:<ukey>                      -> raw value bytes
;   hash     H:<lp ukey><field>            -> field value      meta H#:<ukey> -> u64 count
;   list     L:<lp ukey><seq:8 order>      -> element bytes    meta L#:<ukey> -> [head:s64][tail:s64][len:u64]
;   set      E:<lp ukey><member>           -> #vu8(1)          meta E#:<ukey> -> u64 card
;   zset     Z:<lp ukey><member>           -> score (ieee f64) meta Z#:<ukey> -> u64 card
;            Zs:<lp ukey><score:8 order><member> -> #vu8(1)     (by-score index for ZRANGE)
;

; ---- small byte helpers ----

(define (s->bv s) (string->utf8 s))
(define (bv->s b) (utf8->string b))

(define enc:one (make-bytevector 1 1))   ; the canonical "present" value #vu8(1)

; u64 big-endian
(define (u64->bytes n)
  (let ((b (make-bytevector 8 0)))
    (bytevector-u64-set! b 0 n (endianness big))
    b))
(define (bytes->u64 b off)
  (bytevector-u64-ref b off (endianness big)))

; signed i64, order-preserving big-endian bytes. Two's-complement
; big-endian sorts wrong (negatives have the high bit set, so they'd sort
; AFTER positives under unsigned byte compare). Flipping just the sign bit
; (XOR byte 0 with 0x80) makes unsigned byte order == signed numeric order
; — no >i64 arithmetic. Used for list seqs (LPUSH grows downward/negative,
; RPUSH upward).
(define (flip-top! b) (bytevector-u8-set! b 0 (bitwise-xor (bytevector-u8-ref b 0) #x80)))
(define (s64->order-bytes n)
  (let ((b (make-bytevector 8 0)))
    (bytevector-s64-set! b 0 n (endianness big))
    (flip-top! b)
    b))
(define (order-bytes->s64 b off)
  (let ((c (subbv b off (+ off 8))))
    (flip-top! c)
    (bytevector-s64-ref c 0 (endianness big))))

; IEEE double -> 8 order-preserving bytes so RocksDB byte order == numeric
; order for ZRANGEBYSCORE. Standard total-order transform done at the byte
; level (no >i64 ints): positive -> set sign bit; negative -> invert all 8
; bytes. Inverse decodes scores back.
(define (invert-all! b)
  (let loop ((i 0))
    (if (< i (bytevector-length b))
        (begin (bytevector-u8-set! b i (bitwise-xor (bytevector-u8-ref b i) #xFF))
               (loop (+ i 1))))))
(define (f64->order-bytes x)
  (let ((b (make-bytevector 8 0)))
    (bytevector-ieee-double-set! b 0 (exact->inexact x) (endianness big))
    (if (>= (bytevector-u8-ref b 0) 128)   ; negative
        (invert-all! b)
        (bytevector-u8-set! b 0 (bitwise-ior (bytevector-u8-ref b 0) #x80)))
    b))
(define (order-bytes->f64 b off)
  (let ((c (subbv b off (+ off 8))))
    (if (>= (bytevector-u8-ref c 0) 128)   ; encoded high bit set => was positive
        (bytevector-u8-set! c 0 (bitwise-xor (bytevector-u8-ref c 0) #x80))
        (invert-all! c))                   ; else => was negative (invert back)
    (bytevector-ieee-double-ref c 0 (endianness big))))

; raw IEEE double <-> 8 bytes (NOT order-transformed; for the by-member
; zset value where we just need to round-trip the exact score).
(define (f64->bytes x)
  (let ((b (make-bytevector 8 0)))
    (bytevector-ieee-double-set! b 0 (exact->inexact x) (endianness big))
    b))
(define (bytes->f64 b off)
  (bytevector-ieee-double-ref b off (endianness big)))

; ---- key builders ----

(define (tag b) (s->bv b))

; type-prefixed point key:  <tag><ukey>
(define (point-key tagbv ukey) (bytevector-append tagbv ukey))

; length-prefixed composite prefix:  <tag><u32be len><ukey>
(define (u32->bytes n)
  (let ((b (make-bytevector 4 0)))
    (bytevector-u32-set! b 0 n (endianness big))
    b))
(define (comp-prefix tagbv ukey)
  (bytevector-append tagbv (u32->bytes (bytevector-length ukey)) ukey))
(define (comp-key tagbv ukey suffix)
  (bytevector-append (comp-prefix tagbv ukey) suffix))

; concrete tags (built once)
(define TAG-DIR (tag "T:"))
(define TAG-STR (tag "S:"))
(define TAG-HSH (tag "H:"))   (define TAG-HSH# (tag "H#:"))
(define TAG-LST (tag "L:"))   (define TAG-LST# (tag "L#:"))
(define TAG-SET (tag "E:"))   (define TAG-SET# (tag "E#:"))
(define TAG-ZME (tag "Z:"))   (define TAG-ZSC (tag "Zs:")) (define TAG-ZST# (tag "Z#:"))

; directory
(define (dir-key ukey) (point-key TAG-DIR ukey))
(define (dir-val type-char deadline)
  (bytevector-append (make-bytevector 1 (char->integer type-char))
                     (u64->bytes deadline)))
(define (dir-val-type dv) (integer->char (bytevector-u8-ref dv 0)))
(define (dir-val-deadline dv) (bytes->u64 dv 1))

; string
(define (str-key ukey) (point-key TAG-STR ukey))

; hash
(define (hash-field-key ukey field) (comp-key TAG-HSH ukey field))
(define (hash-prefix ukey) (comp-prefix TAG-HSH ukey))
(define (hash-meta-key ukey) (point-key TAG-HSH# ukey))
; recover the field bytes from a full hash field key, given the user key
(define (hash-field-of fullkey ukey)
  (let ((plen (bytevector-length (hash-prefix ukey))))
    (subbv fullkey plen (bytevector-length fullkey))))

; list
(define (list-elem-key ukey seq) (comp-key TAG-LST ukey (s64->order-bytes seq)))
(define (list-prefix ukey) (comp-prefix TAG-LST ukey))
(define (list-meta-key ukey) (point-key TAG-LST# ukey))
(define (list-meta head tail len)
  (let ((b (make-bytevector 24 0)))
    (bytevector-s64-set! b 0 head (endianness big))
    (bytevector-s64-set! b 8 tail (endianness big))
    (bytevector-u64-set! b 16 len (endianness big))
    b))
(define (list-meta-head m) (bytevector-s64-ref m 0 (endianness big)))
(define (list-meta-tail m) (bytevector-s64-ref m 8 (endianness big)))
(define (list-meta-len  m) (bytevector-u64-ref m 16 (endianness big)))
(define (list-seq-of fullkey ukey)
  (order-bytes->s64 fullkey (bytevector-length (list-prefix ukey))))

; set
(define (set-member-key ukey m) (comp-key TAG-SET ukey m))
(define (set-prefix ukey) (comp-prefix TAG-SET ukey))
(define (set-meta-key ukey) (point-key TAG-SET# ukey))
(define (set-member-of fullkey ukey)
  (subbv fullkey (bytevector-length (set-prefix ukey)) (bytevector-length fullkey)))

; zset
(define (zset-member-key ukey m) (comp-key TAG-ZME ukey m))
(define (zset-member-prefix ukey) (comp-prefix TAG-ZME ukey))
(define (zset-score-key ukey score m)
  (bytevector-append (comp-prefix TAG-ZSC ukey) (f64->order-bytes score) m))
(define (zset-score-prefix ukey) (comp-prefix TAG-ZSC ukey))
(define (zset-meta-key ukey) (point-key TAG-ZST# ukey))
; from a Zs:<lp ukey><score:8><member> key, recover (values score member)
(define (zset-score-of fullkey ukey)
  (order-bytes->f64 fullkey (bytevector-length (zset-score-prefix ukey))))
(define (zset-member-of-score-key fullkey ukey)
  (let ((off (+ (bytevector-length (zset-score-prefix ukey)) 8)))
    (subbv fullkey off (bytevector-length fullkey))))

; ---- bytevector slice (subbv start end) ----
(define (subbv b start end)
  (let* ((n (- end start)) (out (make-bytevector n 0)))
    (let loop ((i 0))
      (if (= i n) out
          (begin (bytevector-u8-set! out i (bytevector-u8-ref b (+ start i)))
                 (loop (+ i 1)))))))

; ---- integer string <-> value (for INCR/DECR and integer replies) ----
; Redis stores numbers as their decimal string form. Returns #f if the
; bytes are not a valid base-10 integer (so callers can emit -ERR).
(define (bytes->int b)
  (let ((s (bv->s b)))
    (let ((n (string->number s 10)))
      (if (and n (integer? n) (exact? n)) n #f))))
(define (int->bytes n) (s->bv (number->string n)))

; float parse for ZADD/ZINCRBY (#f on failure)
(define (bytes->float b)
  (let ((n (string->number (bv->s b))))
    (if (and n (real? n)) (exact->inexact n) #f)))
; Redis prints whole floats without a trailing ".0" (e.g. "3"), else the
; shortest round-tripping decimal.
(define (float->bytes x)
  (if (and (integer? x) (not (infinite? x)))
      (s->bv (number->string (exact (round x))))
      (s->bv (number->string x))))
