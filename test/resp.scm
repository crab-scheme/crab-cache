; test/resp.scm — RESP2 codec: encode exactness + decode incl. pipelining
; and partial-frame handling.

(define (enc->str r) (utf8->string (resp-encode r)))

; decode a string buffer -> (cons list-of-string-commands remainder-string)
(define (parse->sexp s)
  (let ((r (resp-parse (string->utf8 s))))
    (cons (map (lambda (cmd)
                 (map (lambda (a)
                        (if (and (pair? a) (eq? (car a) 'protocol-error)) a
                            (utf8->string a)))
                      cmd))
               (car r))
          (utf8->string (cdr r)))))

(define (test-resp)
  (section "resp encode")
  (check "ok"        "+OK\r\n"            (enc->str (r-ok)))
  (check "simple"    "+PONG\r\n"          (enc->str (r-simple "PONG")))
  (check "err"       "-ERR bad\r\n"       (enc->str (r-err "ERR bad")))
  (check "int"       ":42\r\n"            (enc->str (r-int 42)))
  (check "int neg"   ":-3\r\n"            (enc->str (r-int -3)))
  (check "bulk"      "$5\r\nhello\r\n"    (enc->str (r-bulk (string->utf8 "hello"))))
  (check "bulk empty" "$0\r\n\r\n"        (enc->str (r-bulk (string->utf8 ""))))
  (check "nil"       "$-1\r\n"            (enc->str (r-nil)))
  (check "nil-array" "*-1\r\n"            (enc->str (r-nil-array)))
  (check "array"     "*2\r\n$1\r\na\r\n$1\r\nb\r\n"
         (enc->str (r-array (list (r-bulk (string->utf8 "a")) (r-bulk (string->utf8 "b"))))))
  (check "array mixed" "*2\r\n:1\r\n$-1\r\n"
         (enc->str (r-array (list (r-int 1) (r-nil)))))

  (section "resp decode")
  (check "ping array"  '(("PING")) (car (parse->sexp "*1\r\n$4\r\nPING\r\n")))
  (check "set array"   '(("SET" "k" "v"))
         (car (parse->sexp "*3\r\n$3\r\nSET\r\n$1\r\nk\r\n$1\r\nv\r\n")))
  (check "pipelined"   '(("PING") ("PING"))
         (car (parse->sexp "*1\r\n$4\r\nPING\r\n*1\r\n$4\r\nPING\r\n")))
  (check "inline"      '(("PING")) (car (parse->sexp "PING\r\n")))
  (check "inline args" '(("SET" "k" "v")) (car (parse->sexp "SET k v\r\n")))
  (check "inline lf"   '(("GET" "x")) (car (parse->sexp "GET x\n")))
  (check "empty line skipped" '(("PING")) (car (parse->sexp "\r\nPING\r\n")))

  ; binary-safe value (contains a CR and a space inside the bulk)
  (let ((r (parse->sexp "*3\r\n$3\r\nSET\r\n$1\r\nk\r\n$3\r\na b\r\n")))
    (check "binary-safe bulk" '(("SET" "k" "a b")) (car r)))

  (section "resp partial frames")
  ; an array missing its last bulk body -> no command, remainder = whole input
  (let ((r (parse->sexp "*3\r\n$3\r\nSET\r\n$1\r\nk\r\n$1\r\n")))
    (check "partial: no cmd" '() (car r))
    (check "partial: remainder kept" "*3\r\n$3\r\nSET\r\n$1\r\nk\r\n$1\r\n" (cdr r)))
  ; completing the frame by concatenating the rest yields the command
  (let ((r (parse->sexp "*3\r\n$3\r\nSET\r\n$1\r\nk\r\n$1\r\nv\r\n")))
    (check "completed" '(("SET" "k" "v")) (car r)))
  ; one full command + a partial second -> first delivered, second held
  (let ((r (parse->sexp "*1\r\n$4\r\nPING\r\n*2\r\n$3\r\nGET")))
    (check "full+partial cmd" '(("PING")) (car r))
    (check "full+partial rem" "*2\r\n$3\r\nGET" (cdr r))))
