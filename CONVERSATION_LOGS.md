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

The collector has a 64 MiB capture budget, 32-item queue, configurable per-attempt limit (default 8 MiB), a 200ms policy deadline and 2s send deadline. Early close, bounds, process failure and delivery failure can leave incomplete or missing archives. Collection does not guarantee lossless traffic auditing. The operations center provides complete/partial/missing states, policy versions, attempt chains and non-identifying aggregate drops.

Operator documentation: [operations-center conversation logs](https://github.com/fjiangming/sub2api-extra/blob/dev/operations-center/docs/conversation-logs.md).

Fresh dev is based on upstream main 530c5e3a. Previous dev billing recovery changes are intentionally not carried forward. The existing companion billing-recovery UI may report unavailable against the fresh native core; this archive does not restore those unrelated custom APIs.

Every dev push triggers Fork Dev Release. Tested five-platform archives and checksums are published to this fork's GitHub Releases; native amd64/arm64 images are published to `ghcr.io/fjiangming/sub2api:<numeric-version>`. Only the highest completed numeric release advances GitHub latest and image aliases `dev` / `latest`. The updater and version menu point to this fork. Source publishing never updates a running server.
