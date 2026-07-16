# Smart

The Smart outbound selects a leaf outbound for every new flow using real
connection observations. TCP and UDP are learned independently. History is
separated by network fingerprint and destination site.

### Structure

```json
{
  "type": "smart",
  "tag": "smart-global",

  "outbounds": [
    "proxy-a",
    "proxy-b"
  ],
  "providers": [
    "provider-a"
  ],
  "exclude": "",
  "include": "",
  "use_all_providers": false,

  "url": "https://www.gstatic.com/generate_204",
  "probe_interval": "10m",
  "probe_timeout": "5s",

  "reach_tests": [
    {
      "tag": "gemini",
      "preset": "gemini",
      "interval": "30m",
      "timeout": "10s",
      "unmeasured_policy": "fallback"
    }
  ],

  "max_attempts": 3,
  "attempt_timeout": "4s",
  "site_stickiness": "10m",
  "switch_margin": 0.08,
  "exploration": 0.08,
  "min_samples": 3,

  "breaker_failures": 3,
  "breaker_cooldown": "2m",
  "half_life": "30m",

  "history_path": "smart-history-smart-global.json",
  "history_retention": "168h",
  "max_history_entries": 50000,

  "interrupt_exist_connections": false
}
```

### Selection model

Smart combines:

* confidence-adjusted connection reliability
* connection and first-byte latency
* latency deviation
* sustained throughput for sites with repeated bulk-transfer samples
* site affinity and switch margin
* bounded exploration
* circuit-breaker state

TCP and UDP use different metric keys and weights. UDP does not inherit a TCP
health result. A site-specific failure updates that site strongly and the
network-wide history weakly, but it does not open the global circuit.

Provider entries and nested outbound groups are expanded to unique leaf
outbounds. Circular group references are ignored.

### Fields

#### outbounds

List of outbound or outbound-group tags.

#### providers

List of [Provider](/configuration/provider) tags.

#### exclude

Regular expression used to exclude provider outbounds.

#### include

Regular expression used to include provider outbounds.

#### use_all_providers

Use all configured providers. The default is `false`.

#### url

URL used for low-frequency active probes. The default is
`https://www.gstatic.com/generate_204`.

#### probe_interval

Active probe interval. The default is `10m`.

#### probe_timeout

Timeout for one active probe. The default is `5s`.

Probes use a fixed worker pool. If every probed candidate fails in the same
round, candidate penalties are suppressed because the local network may be
unavailable.

#### reach_tests

Optional application reachability tests. Each test is matched only to its
configured destination domains. A candidate blocked for Gemini remains eligible
for ChatGPT, GitHub and unrelated sites. Tests use independent HTTP requests;
user traffic is not intercepted or replayed.

Built-in presets are `gemini`, `chatgpt`, `claude`, and `git-ssh`. Preset fields
may be overridden explicitly. Every entry supports:

* `tag`: unique test name; defaults to the preset name.
* `preset`: optional built-in defaults.
* `domains`: exact or suffix-matched destination domains.
* `url`: HTTP or HTTPS probe URL.
* `interval`: per-test interval; default `5m`.
* `timeout`: per-candidate timeout; default `10s`.
* `accepted_status`: HTTP status codes considered reachable.
* `blocked_status`: HTTP status codes considered service-blocked.
* `request_headers`: request headers used only by the probe.
* `reachable_body`: body markers required for a reachable result.
* `blocked_body`: body markers that identify service blocking.
* `blocked_headers`: response-header markers that identify service blocking.
* `unmeasured_policy`: `allow`, `fallback`, `suspend`, or `reject`; default
  `fallback`.
* `max_body_bytes`: response-body inspection limit; default 128 KiB and capped
  at 1 MiB.

Candidate states are `reachable`, `blocked`, `unreachable`, and `unmeasured`.
Service results do not open the transport circuit breaker. A temporary or
persistent manual selection deliberately bypasses service exclusion. All Smart
groups in one process share a maximum of eight concurrent reach probes.

Startup, provider changes, and manual force operations probe every candidate.
During stable operation, each `interval` prioritizes the current candidate, the
top eight ranked candidates, unmeasured candidates, and candidates whose result
is due. Reachable results refresh within three intervals; blocked or unreachable
results retry within four intervals. Results that are not due are retained, and
candidates removed by a provider are pruned from reach status.

#### max_attempts

Maximum candidates tried while establishing one new flow. The default is `3`.
Application data is never replayed after a connection has been established.

#### attempt_timeout

Maximum time allowed for one TCP or UDP establishment attempt. The default is
`4s`.

#### site_stickiness

How long a healthy site prefers its current outbound. The default is `10m`.

#### switch_margin

Keep the current or site-affine candidate while its score is within this
distance of the best score. The default is `0.08`. Set to `0` to disable the
margin.

#### exploration

Bounded exploration strength for candidates with little data. The default is
`0.08`. Set to `0` to disable exploration.

#### min_samples

Samples required before a candidate leaves warm-up. The default is `3`.

#### breaker_failures

Consecutive failures required to open a site or global circuit. The default is
`3`.

#### breaker_cooldown

Initial circuit-breaker cooldown. Repeated failures increase the cooldown up
to a bounded exponential multiplier. The default is `2m`.

Only one real-flow recovery trial is admitted for the same half-open
candidate/site/network/transport at a time.

#### half_life

Half-life used to decay old observations. The default is `30m`.

#### history_path

JSON history file. The default is
`smart-history-<sanitized-group-tag>.json` in the working directory.

Network and site identities are hashed before persistence.

#### history_retention

Maximum age of persisted observations. The default is `168h`.

#### max_history_entries

Maximum persisted metric entries. The default is `50000`.

#### interrupt_exist_connections

Interrupt external inbound connections when the selected candidate changes.
The default is `false`. Keep it disabled when graceful flow continuity is
required.

### Clash API

The proxy response includes a `smart` object containing:

```text
selected
pinned
network
site
reason
updated_at
candidate_count
candidate_details_count
candidate_details_truncated
state_counts
reach_tests
candidates (top 32)
```

Each candidate entry contains its state, score, reliability, latency,
first-byte time, throughput, sample count and reason.

`candidate_count` and `state_counts` cover the complete leaf candidate set.
The details remain capped at 32; `candidate_details_truncated` makes that cap
explicit.

Each reach-test status includes its domains, last check time, complete state
counts, candidate count, and up to 32 candidate details. Request headers are
never returned by the status API.

When a remote rule-set uses Smart as `download_detour`, configure the
rule-set's explicit `download_fallback_detour` if bootstrap downloads must
survive a cold Smart group. No implicit direct fallback is performed.

Use the normal proxy update endpoint to pin a candidate. Send an empty name to
clear the pin.

Zashboard (v3.15.0 validated) sees Smart as a standard selectable group without
a fork or injected frontend code. Selecting a candidate creates a 30-minute
temporary override; select `♻️ 智能选择` to clear both the temporary override and
persistent pin and resume automatic ranking. A page reload reads state from the
Clash API instead of relying on browser-local state.

When several Smart groups exist, automatic entries are made globally unique as
`♻️ 智能选择 · <group-tag>` so Zashboard delay tests cannot resolve to the wrong
group. A single Smart group keeps the shorter `♻️ 智能选择` label.

The API accepts `temporary`, `ttl`, `persistent`, and `reason` in addition to
`name`. TTL is limited to 60-86400 seconds. Temporary override metadata is
reported as `temporary_override`, `override_expires_at`,
`override_remaining_seconds`, and `override_reason`.

### Graceful reload

`SIGHUP` prepares and starts a private runtime generation before publication.
Existing TCP and UDP flows keep their previous generation; new flows use the
new generation.

Outbounds, providers, route rules, rule sets and DNS runtime configuration can
be reloaded. Listener, inbound, TUN, endpoint, service, NTP, certificate, log
or experimental API changes return `restart-required` and need a process
restart.
