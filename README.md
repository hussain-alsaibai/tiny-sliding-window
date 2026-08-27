# tiny-sliding-window

> **Zero-dependency sliding window rate limiter. Thread-safe, O(log n), optional persistence. No Redis, no external service.**

[![Python 3.8+](https://img.shields.io/badge/python-3.8+-blue.svg)](https://python.org)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](LICENSE)
[![Zero Dependencies](https://img.shields.io/badge/dependencies-zero-brightgreen.svg)](tiny_sliding_window.py)

## Why?

Fixed-window rate limiting (Redis `INCR` + `EXPIRE`) has a burst problem at window boundaries:

```
Window 1: ────────────────────────────────
Window 2:                               ────────────────────────────────
Allowed:  50  50  50  50  50 → burst at midnight → 50  50  50  50  50
```

Sliding window rate limiting smooths this out. `tiny-sliding-window` implements it in pure Python with sorted timestamps — no Redis, no SaaS, no dependencies.

```python
from tiny_sliding_window import SlidingWindow

# 60 GitHub API calls per minute
github_limiter = SlidingWindow(limit=60, window=60.0, name="github")

for issue in issues:
    result = github_limiter.check()
    if result.allowed:
        fetch_issue(issue)
    else:
        time.sleep(result.wait_ms / 1000)
        github_limiter.check()  # now allowed
```

## Features

| Feature | Description |
|---------|-------------|
| **Sliding window** | O(log n) timestamp-based algorithm — no burst at boundaries |
| **Thread-safe** | `threading.RLock` guards all mutations |
| **Token bucket** | Alternative `TokenBucket` for smooth refill rates |
| **Multi-key limiter** | Combine multiple windows (per-IP + global) |
| **File persistence** | `PersistedSlidingWindow` survives process restarts |
| **Rich analytics** | Hit rate, deny rate, avg wait time, uptime stats |
| **Blocking acquire** | `acquire()` blocks until a permit is available |
| **CLI** | Interactive demo + token bucket visualization |
| **Zero dependencies** | Stdlib only — `threading`, `time`, `bisect`, `statistics`, `json` |

## Install

```bash
pip install tiny-sliding-window
```

Or copy `tiny_sliding_window.py` — it's one file.

## Quick Start

### Sliding Window

```python
from tiny_sliding_window import SlidingWindow
import time

# 10 requests per 3 seconds
limiter = SlidingWindow(limit=10, window=3.0, name="my_api")

for i in range(20):
    result = limiter.check()
    if result.allowed:
        print(f"✅ Request {i+1} allowed ({result.current_hits}/{result.limit})")
    else:
        print(f"❌ Rate limited — wait {result.wait_ms:.0f}ms")
        time.sleep(result.wait_ms / 1000)
        limiter.check()  # retry after waiting
```

### Blocking acquire

```python
limiter = SlidingWindow(limit=5, window=1.0)

# Block until permit is available (rate limiting + backoff handled internally)
limiter.acquire()
do_request()
```

### Token bucket (smooth refill)

```python
from tiny_sliding_window import TokenBucket

# 5 tokens, refill 1 per second. Good for APIs that allow burst + sustained rate.
bucket = TokenBucket(capacity=5, refill_rate=1.0)

for _ in range(10):
    if bucket.check():
        call_api()
    else:
        wait_ms = bucket.wait_for_token()
        print(f"Waited {wait_ms:.0f}ms for token")
        call_api()
```

### Multi-key (per-IP + global)

```python
from tiny_sliding_window import SlidingWindow, MultiKeyLimiter

global_limit = SlidingWindow(limit=1000, window=60.0, name="global")
ip_limits = {}  # keyed by IP

def check_request(ip: str):
    if ip not in ip_limits:
        ip_limits[ip] = SlidingWindow(limit=60, window=60.0, name=f"ip:{ip}")
    
    multi = MultiKeyLimiter([global_limit, ip_limits[ip]])
    return multi.check()
```

### Persisted (survives restarts)

```python
from tiny_sliding_window import PersistedSlidingWindow

limiter = PersistedSlidingWindow(
    limit=100, window=3600.0,
    path="/tmp/my_limiter.json",
    name="hourly_api",
)

# Timestamps survive process restart
result = limiter.check()
```

## CLI Demo

```bash
# Sliding window demo: 5 req / 5s
python tiny_sliding_window.py limiter --limit 5 --window 5.0 --interval 0.3

# Token bucket demo: 5 capacity, 1/sec refill
python tiny_sliding_window.py bucket --capacity 5 --rate 1.0 --interval 0.2

# Stats from persisted file
python tiny_sliding_window.py stats /tmp/my_limiter.json
```

## Analytics

```python
stats = limiter.stats()
print(json.dumps(stats, indent=2))
```

```json
{
  "name": "github_api",
  "limit": 60,
  "window_sec": 60.0,
  "hits": 1247,
  "allows": 1200,
  "denies": 47,
  "hit_rate": 96.23,
  "avg_wait_ms": 284.5,
  "current_hits": 12,
  "windows_since_start": 20.8,
  "uptime_sec": 1247.3
}
```

## Algorithm

```
Sliding Window (sorted timestamps):

  now ──────────────────────────────────────────────►
  
  |←─────────── window (60s) ────────────→|
  [t1] [t2] [t3] ... [t58] [t59] [t60]   ← timestamps

  check():
    1. evict timestamps < now - window
    2. if len(timestamps) < limit: allow + append now
    3. else: deny + return wait_ms until oldest expires

  Complexity: O(log n) insert, O(log n) evict (bisect)
  Memory: O(n) where n = limit × concurrent windows
```

## Ecosystem

Part of the **tiny-\* ecosystem** — zero-dependency, single-file Python libraries for AI agents and autonomous workflows:

- [tiny-agent](https://github.com/hussain-alsaibai/tiny-agent) — Agent runtime with ReAct loop
- [tiny-llm](https://github.com/hussain-alsaibai/tiny-llm) — Unified LLM client
- [tiny-workflow](https://github.com/hussain-alsaibai/tiny-workflow) — DAG orchestration
- [tiny-mcp-observability](https://github.com/hussain-alsaibai/tiny-mcp-observability) — Trace + metrics MCP server
- [tiny-budget](https://github.com/hussain-alsaibai/tiny-budget) — Runtime budget enforcement
- [fast-cache](https://github.com/hussain-alsaibai/fast-cache) — LRU + TTL cache
- [tiny-log](https://github.com/hussain-alsaibai/tiny-log) — Structured logging

---

MIT License © 2026 Hussain Al-Saibai
