; server/pubsub.scm — the per-node pub/sub broker actor.
;
;   (spawn-source "(include \"src/server/pubsub.scm\")" 'broker-main NODE-NAME PEERS)
;
; Holds channel -> subscriber pushers and pattern -> subscriber pushers (the
; pushers are the per-connection socket-writer actors). Pub/sub is GLOBAL in
; Redis (not slot-routed), so PUBLISH delivers to local subscribers AND
; node-sends the publish to every peer broker, which delivers to ITS locals
; (best-effort, NOT through Raft — matching Redis). Subscribers register their
; pusher pid; on disconnect the conn-actor sends (cleanup pusher) to purge it.
;
; Messages (all on the mailbox):
;   (subscribe   pusher chan)      (unsubscribe   pusher chan)
;   (psubscribe  pusher pat)       (punsubscribe  pusher pat)
;   (cleanup     pusher)
;   (publish     chan payload reply-to)   -> reply local-receiver count
;   (rpublish    chan payload)            -> from a peer; deliver locally only
;   (numsub      chan reply-to)           (channels reply-to)
;
; Depends on: reply.scm, encoding.scm, resp.scm.

(include "src/reply.scm")
(include "src/encoding.scm")
(include "src/resp.scm")

; ---- tiny string glob (* ?) for PSUBSCRIBE patterns ----
(define (glob-str? pat str)
  (let ((pn (string-length pat)) (sn (string-length str)))
    (let loop ((pi 0) (si 0))
      (cond
        ((= pi pn) (= si sn))
        ((char=? (string-ref pat pi) #\*)
         (or (loop (+ pi 1) si) (and (< si sn) (loop pi (+ si 1)))))
        ((and (< si sn) (char=? (string-ref pat pi) #\?)) (loop (+ pi 1) (+ si 1)))
        ((and (< si sn) (char=? (string-ref pat pi) (string-ref str si))) (loop (+ pi 1) (+ si 1)))
        (else #f)))))

(define (broker-main node-name peers)
  (let ((chans '())   ; alist: channel-string -> list of pusher-pids
        (pats  '()))  ; alist: pattern-string  -> list of pusher-pids
    (define (subs key al) (let ((e (assoc key al))) (if e (cdr e) '())))
    (define (drop-key key al)
      (cond ((null? al) '()) ((equal? (caar al) key) (cdr al))
            (else (cons (car al) (drop-key key (cdr al))))))
    (define (set-subs key pids al)
      (if (null? pids) (drop-key key al) (cons (cons key pids) (drop-key key al))))
    (define (drop-pid pid lst)
      (cond ((null? lst) '()) ((equal? (car lst) pid) (drop-pid pid (cdr lst)))
            (else (cons (car lst) (drop-pid pid (cdr lst))))))
    (define (has-pid? pid lst)
      (cond ((null? lst) #f) ((equal? (car lst) pid) #t) (else (has-pid? pid (cdr lst)))))
    (define (add-pid pid lst) (if (has-pid? pid lst) lst (cons pid lst)))
    (define (push! pid bytes) (guard (e (#t #f)) (send pid bytes)))

    (define (msg-frame chan payload)
      (resp-encode (r-array (list (r-bulk (string->utf8 "message"))
                                  (r-bulk (string->utf8 chan)) (r-bulk payload)))))
    (define (pmsg-frame pat chan payload)
      (resp-encode (r-array (list (r-bulk (string->utf8 "pmessage")) (r-bulk (string->utf8 pat))
                                  (r-bulk (string->utf8 chan)) (r-bulk payload)))))

    ; deliver to local subscribers; return the count reached
    (define (deliver-local chan payload)
      (let ((n 0))
        (for-each (lambda (p) (push! p (msg-frame chan payload)) (set! n (+ n 1)))
                  (subs chan chans))
        (for-each (lambda (e)
                    (if (glob-str? (car e) chan)
                        (for-each (lambda (p) (push! p (pmsg-frame (car e) chan payload)) (set! n (+ n 1)))
                                  (cdr e))))
                  pats)
        n))
    (define (fan-out chan payload)
      (for-each (lambda (pn)
                  (guard (e (#t #f))
                    (node-send (symbol->string node-name) (symbol->string pn)
                               (list 'broker-publish chan payload))))
                peers))
    (define (active-channels)
      (let loop ((al chans) (acc '()))
        (cond ((null? al) (reverse acc))
              ((null? (cdar al)) (loop (cdr al) acc))
              (else (loop (cdr al) (cons (caar al) acc))))))

    (table-insert! 'cc-broker (symbol->string node-name) (self))
    (let loop ()
      (let ((m (raw-receive)))
        (if (pair? m)
            (case (car m)
              ((subscribe)
               (set! chans (set-subs (caddr m) (add-pid (cadr m) (subs (caddr m) chans)) chans)))
              ((unsubscribe)
               (set! chans (set-subs (caddr m) (drop-pid (cadr m) (subs (caddr m) chans)) chans)))
              ((psubscribe)
               (set! pats (set-subs (caddr m) (add-pid (cadr m) (subs (caddr m) pats)) pats)))
              ((punsubscribe)
               (set! pats (set-subs (caddr m) (drop-pid (cadr m) (subs (caddr m) pats)) pats)))
              ((cleanup)
               (let ((pid (cadr m)))
                 (set! chans (map (lambda (e) (cons (car e) (drop-pid pid (cdr e)))) chans))
                 (set! pats  (map (lambda (e) (cons (car e) (drop-pid pid (cdr e)))) pats))))
              ((publish)
               (let ((n (deliver-local (cadr m) (caddr m))))
                 (fan-out (cadr m) (caddr m))
                 (send (cadddr m) n)))
              ((rpublish) (deliver-local (cadr m) (caddr m)))
              ((numsub)   (send (caddr m) (length (subs (cadr m) chans))))
              ((channels) (send (cadr m) (active-channels)))
              (else #f)))
        (loop)))))
