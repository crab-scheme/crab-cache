; commands/transaction.scm — server-side apply of a MULTI/EXEC transaction.
;
; A whole transaction reaches the shard as ONE command of exactly two parts:
;     (#"EXEC-TXN" <blob>)
; where <blob> is the queued sub-commands serialized back into RESP wire format
; (concatenated *argc\r\n$len\r\narg\r\n... frames). It MUST be a single
; bytevector, not a list of command-lists: the Raft log entry is shipped to
; followers via AppendEntries over node-send, and nested Scheme lists do not
; round-trip across that boundary — a new leader/follower would then apply
; corrupted data (Elle catches this as a phantom "no transaction wrote K V").
; A flat (name . (bytevector)) entry replicates intact.
;
; The conn proposes it as a SINGLE Raft log entry, so the whole batch commits and
; applies ATOMICALLY (full MULTI/EXEC isolation). At apply we parse the blob back
; into sub-commands and run each in order against the same ctx — so a read later
; in the transaction sees writes made earlier — returning their replies as a RESP
; array, exactly mirroring Redis EXEC. All writes land under one apply, so
; group-commit fsyncs them together and the conn gets one array reply.

(define (cmd-exec-txn ctx operands)
  (let ((subcmds (car (resp-parse (car operands)))))
    (r-array (map (lambda (sub) (shard-dispatch ctx sub)) subcmds))))

(register-command! "EXEC-TXN" cmd-exec-txn)
