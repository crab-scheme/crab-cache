; slotmap.scm — Redis-compatible key→slot hashing + slot→shard partitioning.
;
; Redis Cluster splits the keyspace into 16384 slots; slot = CRC16(key) mod
; 16384, with the {hashtag} rule so related keys co-locate. Matching this
; byte-for-byte is what lets `redis-cli -c` and any cluster-aware client talk
; to crab-cache unmodified.
;
; Depends on: encoding.scm (subbv).

(define CLUSTER-SLOTS 16384)

; CRC16-CCITT (XMODEM): polynomial 0x1021, init 0x0000, no input/output
; reflection, no final XOR — the exact function Redis Cluster uses.
(define (crc16-bytes bv)
  (let ((n (bytevector-length bv)))
    (let loop ((i 0) (crc 0))
      (if (= i n)
          crc
          (let ((c (bitwise-and #xFFFF
                                (bitwise-xor crc (arithmetic-shift (bytevector-u8-ref bv i) 8)))))
            (let bit ((j 0) (crc c))
              (if (= j 8)
                  (loop (+ i 1) crc)
                  (bit (+ j 1)
                       (bitwise-and #xFFFF
                                    (if (= 0 (bitwise-and crc #x8000))
                                        (arithmetic-shift crc 1)
                                        (bitwise-xor (arithmetic-shift crc 1) #x1021)))))))))))

; {hashtag}: if the key has a '{' followed later by a '}' with at least one
; byte between them, hash ONLY that substring; otherwise hash the whole key.
; (Redis: first '{', then the first '}' after it; empty {} hashes the key.)
(define (hashtag-bytes bv)
  (let ((n (bytevector-length bv)))
    (let find-open ((i 0))
      (cond
        ((>= i n) bv)
        ((= (bytevector-u8-ref bv i) 123)          ; '{'
         (let find-close ((j (+ i 1)))
           (cond
             ((>= j n) bv)                          ; no '}' -> whole key
             ((= (bytevector-u8-ref bv j) 125)      ; '}'
              (if (> j (+ i 1)) (subbv bv (+ i 1) j) bv))  ; empty {} -> whole key
             (else (find-close (+ j 1))))))
        (else (find-open (+ i 1)))))))

(define (key-slot bv) (modulo (crc16-bytes (hashtag-bytes bv)) CLUSTER-SLOTS))

; ---- slot -> shard partitioning (equal contiguous ranges) ----

(define (slot->shard slot nshards) (quotient (* slot nshards) CLUSTER-SLOTS))

; shard -> inclusive (start . end) slot range
(define (shard-slot-range shard nshards)
  (cons (quotient (* shard CLUSTER-SLOTS) nshards)
        (- (quotient (* (+ shard 1) CLUSTER-SLOTS) nshards) 1)))

(define (key-shard bv nshards) (slot->shard (key-slot bv) nshards))
