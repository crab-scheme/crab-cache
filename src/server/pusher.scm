; server/pusher.scm — the sole socket WRITER for a subscribed connection.
;
;   (spawn-source "(include \"src/server/pusher.scm\")" 'pusher SOCK)
;
; A subscribed connection has two concurrent write sources: the broker pushing
; messages, and the conn-actor's own subscribe/unsubscribe/ping confirmations.
; Routing BOTH through this one actor makes it the single writer, so RESP
; frames never interleave. It receives pre-encoded bytevectors (and 'stop) on
; its mailbox and writes them in order. The conn-actor keeps reading commands
; on its own thread — TCP is full-duplex, so concurrent read (conn) + write
; (pusher) on the same fd is fine.

(define (pusher sock)
  (let loop ()
    (let ((m (raw-receive)))
      (cond
        ((eq? m 'stop) (guard (e (#t #f)) (tcp-close sock)))
        ((bytevector? m) (guard (e (#t #f)) (tcp-send sock m)) (loop))
        (else (loop))))))
