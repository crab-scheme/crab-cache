; server/conn.scm — the per-connection actor body.
;
; One spawn-source actor (its OWN OS thread) per accepted client socket, so
; the blocking tcp-recv that waits for the next request only parks THIS
; connection's thread — never a shared worker (design §10 + the net
; clone-then-unlock fix means concurrent connections don't serialize).
;
; It reads RESP bytes, decodes complete commands (holding any partial tail
; for the next read), forwards each command to the shard-owner actor, and
; writes the batched replies back. Pure protocol glue — zero command
; semantics (those live in the shard-owner).
;
; Loaded into a fresh per-actor runtime via:  (spawn-source "(include …)" 'conn sock)

(include "src/reply.scm")
(include "src/encoding.scm")        ; subbv
(include "src/resp.scm")

(define RECV-MAX 65536)

; Hand-rolled synchronous RPC to the shard-owner (the beam prelude's `call`
; isn't auto-loaded): send (self . cmd), block for the single reply. The
; conn-actor has exactly one request outstanding at a time, so the next
; mailbox message is always this command's reply.
(define (ask owner cmd)
  (send owner (cons (self) cmd))
  (raw-receive))

(define (conn sock)
  (let ((owner (table-lookup 'crabcache "owner")))
    (let loop ((buf (make-bytevector 0 0)))
      (let ((chunk (tcp-recv sock RECV-MAX)))
        (if (= (bytevector-length chunk) 0)
            (tcp-close sock)                         ; clean EOF: client gone
            (let* ((data   (bytevector-append buf chunk))
                   (parsed (resp-parse data))
                   (cmds   (car parsed))
                   (rem    (cdr parsed))
                   (result (serve-commands owner cmds)))
              ; result = (out-bytes . keep-open?)
              (if (> (bytevector-length (car result)) 0)
                  (tcp-send sock (car result)))
              (if (cdr result)
                  (loop rem)
                  (tcp-close sock))))))))            ; protocol error -> close

; Build the concatenated reply bytes for a batch of decoded commands. A
; protocol-error frame is answered then the connection is closed (Redis
; behavior). Returns (out-bytevector . keep-open?).
(define (serve-commands owner cmds)
  (let loop ((cs cmds) (out (make-bytevector 0 0)))
    (if (null? cs)
        (cons out #t)
        (let ((cmd (car cs)))
          (if (and (pair? cmd) (eq? (car cmd) 'protocol-error))
              (cons (bytevector-append out (resp-encode (r-err (cadr cmd)))) #f)
              (loop (cdr cs)
                    (bytevector-append out (resp-encode (ask owner cmd)))))))))
