; self-verify runner for the RESP2 codec (pure Scheme; no store/network).
(include "src/reply.scm")
(include "src/encoding.scm")     ; for subbv
(include "src/resp.scm")
(include "test/harness.scm")
(include "test/resp.scm")
(test-resp)
(done!)
