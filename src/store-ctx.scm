; store-ctx.scm — the shard context + all RocksDB access + the keyspace
; directory (existence / type / TTL) + key purge.
;
; This is the ONLY layer that touches the store-* host procedures. Command
; modules go through the helpers here, so they never see a CF name or a
; handle — they see logical keys and the directory.
;
; Depends on: encoding.scm.

; A shard context: the open store handle, the column family this shard's
; data lives in (Phase 3 = "default"; Phase 5 gives each shard its own CF),
; and a mutable logical clock (a 1-slot vector) advanced by the active-
; expiry actor. The clock is what TTL deadlines are compared against, so
; every replica expires a key at the same logical tick (DD-3).
(define-record-type shard-ctx
  ; accessors auto-derived: shard-ctx-handle/-cf/-sync/-clock, plus the
  ; clock mutator set-shard-ctx-clock! (CrabScheme record-type shorthand).
  (fields (immutable handle)
          (immutable cf)
          (immutable sync)            ; fsync each write? (durable mode)
          (mutable clock)))

; (make-ctx handle [cf] [sync?])
(define (make-ctx handle . opts)
  (make-shard-ctx handle
                  (if (and (pair? opts) (car opts)) (car opts) "default")
                  (and (pair? opts) (pair? (cdr opts)) (cadr opts))
                  0))

(define (ctx-clock-advance! ctx d)
  (set-shard-ctx-clock! ctx (+ (shard-ctx-clock ctx) d)))
(define (clock ctx) (shard-ctx-clock ctx))

; ---- raw store ops on this shard's CF ----

(define (kv-get ctx k)    (store-get (shard-ctx-handle ctx) (shard-ctx-cf ctx) k))
(define (kv-put! ctx k v) (store-put (shard-ctx-handle ctx) (shard-ctx-cf ctx) k v (shard-ctx-sync ctx)))
(define (kv-del! ctx k)   (store-delete (shard-ctx-handle ctx) (shard-ctx-cf ctx) k (shard-ctx-sync ctx)))
(define (kv-exists? ctx k)    (and (kv-get ctx k) #t))

; Prefix scan -> list of (fullkey . value) bytevector pairs, over a stable
; snapshot taken at iter time.
(define (kv-scan ctx prefix)
  (let ((it (store-iter (shard-ctx-handle ctx) (shard-ctx-cf ctx) prefix)))
    (let loop ((acc '()))
      (let ((nx (store-iter-next it)))
        (if nx
            (loop (cons nx acc))
            (begin (store-iter-close it) (reverse acc)))))))

(define (kv-scan-count ctx prefix)
  (let ((it (store-iter (shard-ctx-handle ctx) (shard-ctx-cf ctx) prefix)))
    (let loop ((n 0))
      (if (store-iter-next it)
          (loop (+ n 1))
          (begin (store-iter-close it) n)))))

; ---- keyspace directory: existence / type / TTL with lazy expiry ----

; deadline 0 = persistent. A non-zero deadline is "dead" once the logical
; clock has reached it.
(define (dead? ctx deadline)
  (and (not (= deadline 0)) (<= deadline (clock ctx))))

; Raw directory read: #f or (cons type-char deadline). No expiry applied.
(define (dir-raw ctx ukey)
  (let ((dv (kv-get ctx (dir-key ukey))))
    (if dv (cons (dir-val-type dv) (dir-val-deadline dv)) #f)))

; Live directory read: applies lazy expiry (purges + returns #f if the
; key's deadline has passed). Returns #f or (cons type-char deadline).
(define (key-entry ctx ukey)
  (let ((e (dir-raw ctx ukey)))
    (cond ((not e) #f)
          ((dead? ctx (cdr e)) (purge-key! ctx ukey) #f)
          (else e))))

(define (key-type ctx ukey)
  (let ((e (key-entry ctx ukey))) (and e (car e))))
(define (key-deadline ctx ukey)
  (let ((e (key-entry ctx ukey))) (and e (cdr e))))
(define (key-exists? ctx ukey) (and (key-entry ctx ukey) #t))

; Establish/replace the directory entry (preserves nothing — callers that
; want to keep a TTL pass it in).
(define (dir-set! ctx ukey type-char deadline)
  (kv-put! ctx (dir-key ukey) (dir-val type-char deadline)))

; Change only the deadline, keeping the type. Returns #t if the key was
; live, #f if absent (EXPIRE on a missing key is a no-op returning 0).
(define (set-deadline! ctx ukey deadline)
  (let ((e (key-entry ctx ukey)))
    (and e (begin
             (dir-set! ctx ukey (car e) deadline)
             ; perf #4: a string gaining a TTL must leave the no-TTL serving map
             ; so its reads route to the shard's lazy-expiry path (PERSIST, i.e.
             ; deadline 0, re-warms lazily on the next shard read).
             (if (and (char=? (car e) #\s) (not (= deadline 0)))
                 (table-delete! 'cc-str ukey))
             #t))))

; Type-guard for a write/read: returns
;   'ok       key is live and of the wanted type (or absent — caller creates)
;   'wrong    key is live but a different type  -> caller emits WRONGTYPE
; `want` is a type char.
(define (type-guard ctx ukey want)
  (let ((t (key-type ctx ukey)))
    (cond ((not t) 'ok)
          ((char=? t want) 'ok)
          (else 'wrong))))

; ---- purge: delete every trace of a key (DEL and lazy/active expiry) ----

(define (del-each ctx pairs)
  (for-each (lambda (kv) (kv-del! ctx (car kv))) pairs))

; Reads the key's type from the raw directory (purge is also called on an
; already-expired entry) and removes its data, meta, and directory record.
(define (purge-key! ctx ukey)
  (let ((e (dir-raw ctx ukey)))
    (if (not e)
        #f
        (let ((t (car e)))
          (cond
            ((char=? t #\s) (kv-del! ctx (str-key ukey)))
            ((char=? t #\h)
             (del-each ctx (kv-scan ctx (hash-prefix ukey)))
             (kv-del! ctx (hash-meta-key ukey)))
            ((char=? t #\l)
             (del-each ctx (kv-scan ctx (list-prefix ukey)))
             (kv-del! ctx (list-meta-key ukey)))
            ((char=? t #\e)
             (del-each ctx (kv-scan ctx (set-prefix ukey)))
             (kv-del! ctx (set-meta-key ukey)))
            ((char=? t #\z)
             (del-each ctx (kv-scan ctx (zset-member-prefix ukey)))
             (del-each ctx (kv-scan ctx (zset-score-prefix ukey)))
             (kv-del! ctx (zset-meta-key ukey))))
          (kv-del! ctx (dir-key ukey))
          ; perf #4: evict from the in-memory serving map (covers DEL, lazy +
          ; active expiry, RENAME, and string->other-type overwrites).
          (table-delete! 'cc-str ukey)
          #t))))

; ---- composite-type (hash/list/set/zset) lifecycle helpers ----
;
; A composite key exists in the directory while it has ≥1 element. Add an
; element -> ensure the directory entry (create with no TTL if absent,
; preserving any existing TTL). Remove an element -> if the collection is
; now empty, purge the whole key.  Callers MUST first type-guard (a write
; to a key holding a different type is WRONGTYPE, not a silent clobber).

(define (ctype-touch! ctx ukey type-char)
  (if (not (key-exists? ctx ukey))
      (dir-set! ctx ukey type-char 0)))

(define (purge-if-empty! ctx ukey count)
  (if (<= count 0) (purge-key! ctx ukey)))

; ---- u64 counter helpers for composite-type meta (lists use L#) ----

(define (counter-get ctx metakey)
  (let ((v (kv-get ctx metakey))) (if v (bytes->u64 v 0) 0)))
(define (counter-set! ctx metakey n)
  (if (= n 0) (kv-del! ctx metakey) (kv-put! ctx metakey (u64->bytes n))))
