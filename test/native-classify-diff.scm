; native-classify-diff.scm — differential test for cc-5pw.2.
;
; The native fast-path (conn-serve-batch, crabscheme beam.rs) classifies each
; RESP frame in Rust via cc_classify. That MUST agree with the interpreted
; router.scm `classify-route` for every command, or the two paths would route
; the same request differently. This fuzzes the Rust `native-classify-route`
; builtin against `classify-route` over a corpus of verbs x operand-shapes x
; shard-counts and asserts they never disagree.
;
;   crabscheme run test/native-classify-diff.scm
(include "src/encoding.scm")
(include "src/reply.scm")
(include "src/slotmap.scm")
(include "src/router.scm")
(include "test/harness.scm")

(define (k s) (string->utf8 s))

; Operand shapes: empty, single key, same-slot pair (shared {hashtag}),
; cross-slot pair, MSET-shaped (key val key val) same- and cross-slot keys, and
; a 3-arg arbitrary run. {t}-tagged keys co-locate; {a}/{b} land on distinct
; slots — so multi-key commands exercise both the all-equal and crossslot arms,
; and MSET exercises even-index key selection (the odd-index values must be
; ignored, which only matches if native mirrors even-index-list exactly).
(define opsets
  (list
   '()
   (list (k "k"))
   (list (k "{t}1") (k "{t}2"))
   (list (k "{a}") (k "{b}"))
   (list (k "{t}1") (k "v") (k "{t}2") (k "w"))
   (list (k "{a}") (k "v") (k "{b}") (k "w"))
   (list (k "a") (k "b") (k "c"))))

; A verb from every route class in key-positions, plus lowercase variants
; (native upcases internally) and an unknown verb (-> single key at operand 0).
(define verbs
  '("PING" "ECHO" "SELECT" "COMMAND" "INFO" "QUIT" "TICK"   ; 'any
    "DBSIZE" "FLUSHALL" "FLUSHDB" "KEYS"                    ; 'all
    "CLUSTER"                                               ; 'cluster
    "DEL" "EXISTS" "UNLINK" "MGET" "SMISMEMBER"             ; all operands keys
    "MSET"                                                  ; even-index keys
    "GET" "SET" "INCR" "LPUSH" "HSET" "ZADD"                ; single key at 0
    "SUBSCRIBE" "PUBLISH" "MULTI" "EXEC" "DISCARD"          ; (still single-key/else)
    "FOOBAR"                                                ; unknown verb
    "ping" "get" "mset" "cluster" "del"))                  ; case-insensitivity

(define nsl '(1 3 16))

(section "native-classify-route == router.scm classify-route")
(display "  corpus: ")
(display (* (length verbs) (length opsets) (length nsl)))
(display " (verb x operands x nshards)") (newline)

(define mism '())
(for-each
 (lambda (ns)
   (for-each
    (lambda (v)
      (for-each
       (lambda (ops)
         (let ((want (classify-route (string-upcase v) ops ns))
               (got  (native-classify-route (string->utf8 v) ops ns)))
           (if (not (equal? want got))
               (set! mism (cons (list 'verb v 'nops (length ops) 'ns ns
                                       'want want 'got got)
                                mism)))))
       opsets))
    verbs))
 nsl)

; One assertion over the whole corpus: the mismatch list must be empty (any
; disagreement is printed in full by the harness).
(check "no native/interpreted classification disagreements" '() (reverse mism))

(done!)
