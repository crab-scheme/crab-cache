; commands/cluster.scm — CLUSTER subcommands, answered at the connection
; level from the node's topology (not through a shard's Raft log).
;
; cfg = (host port nshards node-id leader-ranges) where leader-ranges is a
; list of (start end host port node-id) describing who serves which slots.
; In Phase 5 (single host) every range points at this node; Phase 6 fills it
; from the live slot-map so `redis-cli -c` follows MOVED to the right node.
;
; Depends on: slotmap.scm, reply.scm, encoding.scm.

(define (cfg-host cfg)    (list-ref cfg 0))
(define (cfg-port cfg)    (list-ref cfg 1))
(define (cfg-nshards cfg) (list-ref cfg 2))
(define (cfg-myid cfg)    (list-ref cfg 3))
(define (cfg-ranges cfg)  (list-ref cfg 4))   ; list of (start end host port id)

(define (cluster-reply operands cfg)
  (if (null? operands)
      (r-wrong-args "cluster")
      (let ((sub (string-upcase (utf8->string (car operands)))))
        (cond
          ((string=? sub "KEYSLOT")
           (if (null? (cdr operands)) (r-wrong-args "cluster|keyslot")
               (r-int (key-slot (cadr operands)))))
          ((string=? sub "MYID") (r-bulk (string->utf8 (cfg-myid cfg))))
          ((string=? sub "INFO")
           (r-bulk (string->utf8
                    (string-append
                     "cluster_enabled:1\r\n"
                     "cluster_state:ok\r\n"
                     "cluster_slots_assigned:16384\r\n"
                     "cluster_slots_ok:16384\r\n"
                     "cluster_known_nodes:" (number->string (count-nodes cfg)) "\r\n"
                     "cluster_size:" (number->string (cfg-nshards cfg)) "\r\n"))))
          ((string=? sub "SLOTS") (cluster-slots cfg))
          ((string=? sub "NODES") (r-bulk (string->utf8 (cluster-nodes-text cfg))))
          ((string=? sub "SHARDS") (r-array '()))
          (else (r-ok))))))

; distinct node ids appearing in the ranges
(define (count-nodes cfg)
  (let loop ((rs (cfg-ranges cfg)) (seen '()))
    (if (null? rs) (length seen)
        (let ((id (list-ref (car rs) 4)))
          (loop (cdr rs) (if (member-str id seen) seen (cons id seen)))))))

; CLUSTER SLOTS -> *[ start end [ip port id] ]
(define (cluster-slots cfg)
  (r-array
   (map (lambda (rng)
          (r-array (list (r-int (list-ref rng 0))
                         (r-int (list-ref rng 1))
                         (r-array (list (r-bulk (string->utf8 (list-ref rng 2)))
                                        (r-int (list-ref rng 3))
                                        (r-bulk (string->utf8 (list-ref rng 4))))))))
        (cfg-ranges cfg))))

; CLUSTER NODES -> one line per node, slots it owns appended.
(define (cluster-nodes-text cfg)
  (let loop ((rs (cfg-ranges cfg)) (out ""))
    (if (null? rs) out
        (let* ((rng (car rs)) (id (list-ref rng 4))
               (host (list-ref rng 2)) (port (list-ref rng 3))
               (self? (string=? id (cfg-myid cfg)))
               (line (string-append
                      id " " host ":" (number->string port) "@" (number->string (+ port 10000))
                      (if self? " myself,master" " master") " - 0 0 0 connected "
                      (number->string (list-ref rng 0)) "-" (number->string (list-ref rng 1))
                      "\r\n")))
          (loop (cdr rs) (string-append out line))))))
