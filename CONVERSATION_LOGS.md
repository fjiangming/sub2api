# Upstream conversation capture

The fork dev release adds opt-in raw upstream application-body observation for the companion operations center. It does not change billing, deduct balances, replay inference or write a raw disk spool.

Core environment variables (leave unset to disable):

```dotenv
CONVERSATION_LOGS_URL=http://operations-center:9872
CONVERSATION_LOGS_TOKEN=<separate-random-token-at-least-32-characters>
```

Set the same token in the operations center and ensure private network reachability. Saving starts disabled; configure excluded users and the save policy in its Conversation Logs tab before enabling. These settings are provided for a later approved deployment; no server configuration is changed by a source release.

Capture uses authenticated user/key IDs, actual HTTP RoundTrips and WebSocket lease/relay boundaries. Original body bytes are not reformatted; HTTP transport auto-decompression may already have occurred, while encoded bodies exposed by the transport remain encoded. Headers, URL query strings and TLS/network frames are outside the archive. Downloads reproduce the captured bytes, including numeric spelling and unknown fields.

Policies are checked before capture, before sending and again before database insertion. Excluded users create no new archive body, metadata, session, index or gap record; in-flight archives are discarded after an exclusion. Existing history follows retention. Policy failure disables the attempt's capture without failing inference. Persistent WebSocket connections denied at their first segment need reconnecting to resume capture.

The operations-center save policy can restrict collection to selected upstream models, with an optional Kimi K3-and-later range. Matching ignores case and whitespace, supports Fable/Opus shorthand and Gemini carbon, and otherwise requires an exact named version. Unknown and unselected models produce no new archive body, metadata, session, index or gap. Enabled empty selection means collect none. Existing encrypted policies default to the previous unrestricted model behavior until explicitly configured; clients omitting the new fields preserve an existing selection. Excluded users remain excluded regardless of model.

Models are identified from observed outbound JSON/multipart, Gemini/Vertex model paths and WebSocket top-level/response model fields, with model inheritance within the same authenticated lease. HTTP assembly and model identification happen in the background after observation ends. All candidates must pass the attempt policy and current send policy; ingress checks users/models before decoding. Unknown or disallowed models never reach persistent storage. Historical archives remain readable, but excluded historical models cannot write reprocessing copies.

Inference always takes priority. Request goroutines use an asynchronously refreshed policy cache; they never wait for archive network, disk, masking or acknowledgments. Refresh runs every second with a 200ms deadline; a failed or more-than-three-second-old cache skips capture. Startup warmup can skip requests, and fresh send/ingress checks reject newly excluded users even while the cache was valid.

There is no configurable per-attempt truncation or 10,000-frame cap in capture v2. HTTP observations use 16 KiB chunks; frame bytes and metadata share the existing 64 MiB global memory budget and 32-item completed queue. WebSocket observers only copy and nonblockingly enqueue frames (16,384 pending entries); a separate worker identifies models and segments messages. Policy/WS lock contention, full queues or exhausted budgets abandon the affected archive rather than wait for logging. Failed scopes stop further observation; earlier completed segments can remain. Individual bodies larger than the available global budget cannot be archived. Assembly, model parsing and delivery need additional bounded-worker heap; 64 MiB is not a total process RSS limit.

One background sender hashes chunks and streams base64 into the unchanged JSON contract, without allocating a full encoded envelope. It retries the same event ID at most three times, with a two-second HTTP timeout per send and fresh policy gates before every send. The receiver and sender verify a SHA-256 digest covering byte lengths, both bodies, and frame order/direction/time/binary flag/body. Successful delivery requires a matching ID/digest acknowledgment. There is no disk spool, crash recovery, guaranteed shutdown drain or inference retry. Process exits, exhausted memory/queue, unreachable archives and failed writes can still lose complete records. No observer lock is held during delivery. Pipe cancellation joins the encoder before releasing body ownership.

This is low-overhead best effort observation, not literally zero-cost capture. Synchronous immutable copying, allocation and shared CPU/GC can still affect first-byte and total latency, especially for large requests or bursts. The companion operations center isolates ingestion, masking, storage and retention in a low-priority single child process, with memory admission and a processing deadline; busy/unavailable workers reject whole archives with no production synchronous fallback. Child crashes/timeouts before persistence may lose an archive. Synthetic benchmarks in `performance_test.go` and the release checks report overhead; they do not guarantee unchanged production P95/P99. See the companion performance review for measurements and limitations.

Early close and read/transport errors preserve only bytes actually observed and remain explicitly incomplete. Complete means observed byte completion, not model success, terminal SSE semantics, billing success or an entire multi-turn conversation. Retention, model/user exclusions and missing session identifiers can make a session discontinuous. The companion UI separates collection, processing and delivery states; incomplete records cannot be pushed. Legacy collectors remain compatible but still truncate, so update both components for capture v2 and check the displayed collector version.

The operations center preserves encrypted source on processing errors (including masked-only mode), offers reprocessing using current rules, and refuses a whole new record when storage is full. Original retained bytes can be downloaded unchanged; previously discarded bytes cannot be recovered. Operator settings, privacy categories and explicit limitations are maintained in the companion documentation.

Operator documentation: [operations-center conversation logs](https://github.com/fjiangming/sub2api-extra/blob/main/operations-center/docs/conversation-logs.md). The companion operations center is released from its main branch as `ghcr.io/fjiangming/sub2api-extra:operations-center-latest`; update it separately from the core application.

Fresh dev is based on upstream main 530c5e3a. Previous dev billing recovery changes are intentionally not carried forward. The existing companion billing-recovery UI may report unavailable against the fresh native core; this archive does not restore those unrelated custom APIs.

Every dev push triggers Fork Dev Release. Tested five-platform archives and checksums are published to this fork's GitHub Releases; native amd64/arm64 images are published to `ghcr.io/fjiangming/sub2api:<numeric-version>`. Only the highest completed numeric release advances GitHub latest and image aliases `dev` / `latest`. The updater and version menu point to this fork. Source publishing never updates a running server.
