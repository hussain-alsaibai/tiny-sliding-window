#!/usr/bin/env python3
"""
tiny-sliding-window — Zero-dependency sliding window rate limiter with analytics.

Sliding window rate limiting is superior to fixed-window (Redis classic) because
it doesn't allow a burst at window boundaries. This implementation is pure
Python stdlib — no Redis, no external service — using an in-process sliding
log with O(log n) lookup.

Use cases:
- Rate limiting MCP tool calls (max 60 calls/minute per agent)
- Bounty scanner: max 100 GitHub API calls/hour
- LLM API: max 50 requests/minute to avoid 429s
- Web scraper: max 10 req/sec per target host

Usage:
    limiter = SlidingWindow(limit=60, window=60.0)  # 60 ops / 60s
    if limiter.check():
        do_the_thing()
    else:
        wait_or_backoff()

    # Analytics
    print(limiter.stats())   # {hits, misses, allows, denies, hit_rate, ...}
"""
from __future__ import annotations

import threading
import time
import bisect
import statistics
import json
import argparse
import sys
from dataclasses import dataclass, field
from typing import Optional, Callable
from pathlib import Path


VERSION = "1.0.0"


# ─── Models ──────────────────────────────────────────────────────────────────

@dataclass
class WindowStats:
    hits: int = 0        # total check() calls
    allows: int = 0      # allowed through
    denies: int = 0       # denied (over limit)
    over_limit_count: int = 0  # times limit was exceeded
    total_wait_ms: float = 0.0  # cumulative wait time
    windows_since_start: int = 0
    _window_start_time: float = 0.0

    def hit_rate(self) -> float:
        return self.allows / self.hits if self.hits else 0.0

    def avg_wait_ms(self) -> float:
        denom = self.over_limit_count or 1
        return self.total_wait_ms / denom

    def reset(self):
        self.__init__()


@dataclass
class LimiterResult:
    allowed: bool
    wait_ms: float        # milliseconds to wait before retrying
    current_hits: int     # hits in current window
    limit: int            # configured limit
    window_sec: float     # configured window


# ─── Core sliding window ──────────────────────────────────────────────────────

class SlidingWindow:
    """
    Thread-safe sliding window rate limiter using sorted timestamps.

    Keeps a sorted list of timestamps. On check():
    1. Evict timestamps older than `window` seconds.
    2. If len(timestamps) < limit: allow + record timestamp.
    3. Else: deny, return wait time until oldest timestamp expires.

    Memory: O(n) in worst case, where n = limit × concurrent windows.
    Time: O(log limit) for insert + O(log limit) for evict (bisect).
    """

    def __init__(
        self,
        limit: int,
        window: float,
        *,
        name: str = "default",
        on_deny: Optional[Callable[[], None]] = None,
    ):
        self.limit = limit
        self.window = window
        self.name = name
        self.on_deny = on_deny

        self._timestamps: list[float] = []
        self._lock = threading.RLock()
        self._stats = WindowStats()
        self._window_start = time.monotonic()

    def _evict_expired(self, now: float) -> int:
        """Remove timestamps older than `window` seconds. Returns count evicted."""
        cutoff = now - self.window
        # bisect_left finds first timestamp > cutoff
        i = bisect.bisect_left(self._timestamps, cutoff)
        if i > 0:
            del self._timestamps[:i]
            return i
        return 0

    def _oldest(self, now: float) -> float:
        """Return the oldest timestamp still in window, or now."""
        if self._timestamps:
            return self._timestamps[0]
        return now

    def check(self) -> LimiterResult:
        """
        Check if a request is allowed. Thread-safe.

        Returns LimiterResult with `allowed`, `wait_ms`, `current_hits`, `limit`.
        """
        with self._lock:
            now = time.monotonic()
            self._evict_expired(now)

            current_hits = len(self._timestamps)

            if current_hits < self.limit:
                # Allow
                self._timestamps.append(now)
                self._stats.hits += 1
                self._stats.allows += 1
                return LimiterResult(
                    allowed=True,
                    wait_ms=0.0,
                    current_hits=current_hits + 1,
                    limit=self.limit,
                    window_sec=self.window,
                )
            else:
                # Deny — compute wait time
                oldest = self._timestamps[0]
                wait_sec = self.window - (now - oldest)
                wait_ms = max(0.0, wait_sec * 1000)

                self._stats.hits += 1
                self._stats.denies += 1
                self._stats.over_limit_count += 1
                self._stats.total_wait_ms += wait_ms

                if self.on_deny:
                    self.on_deny()

                return LimiterResult(
                    allowed=False,
                    wait_ms=wait_ms,
                    current_hits=current_hits,
                    limit=self.limit,
                    window_sec=self.window,
                )

    def acquire(self) -> bool:
        """
        Block until a permit is acquired. Returns True.
        Like `threading.Semaphore.acquire()` but rate-limited.
        """
        while True:
            result = self.check()
            if result.allowed:
                return True
            time.sleep(result.wait_ms / 1000)

    def wait_if_needed(self) -> float:
        """
        Check and wait if needed. Returns actual wait time in ms.
        Always returns after allowing one request through.
        """
        result = self.check()
        if result.allowed:
            return 0.0
        time.sleep(result.wait_ms / 1000)
        # After sleeping, try again
        self.check()
        return result.wait_ms

    def get_current_hits(self) -> int:
        """Return current hits in window without modifying state."""
        with self._lock:
            now = time.monotonic()
            self._evict_expired(now)
            return len(self._timestamps)

    def stats(self) -> dict:
        """Return a snapshot of stats."""
        with self._lock:
            elapsed = time.monotonic() - self._window_start
            complete_windows = elapsed / self.window if self.window > 0 else 0
            return {
                "name": self.name,
                "limit": self.limit,
                "window_sec": self.window,
                "hits": self._stats.hits,
                "allows": self._stats.allows,
                "denies": self._stats.denies,
                "hit_rate": round(self._stats.hit_rate() * 100, 2),
                "avg_wait_ms": round(self._stats.avg_wait_ms(), 2),
                "current_hits": len(self._timestamps),
                "windows_since_start": round(complete_windows, 2),
                "uptime_sec": round(elapsed, 1),
            }

    def reset(self):
        """Clear all timestamps and stats. Thread-safe."""
        with self._lock:
            self._timestamps.clear()
            self._stats.reset()
            self._window_start = time.monotonic()

    def __repr__(self):
        return f"SlidingWindow({self.limit}/{self.window}s, current={self.get_current_hits()})"


# ─── Composite multi-key limiter ─────────────────────────────────────────────

class MultiKeyLimiter:
    """
    Combines multiple SlidingWindows for different dimensions.
    E.g., per-IP + global: max 10/min per IP and 100/min total.
    """

    def __init__(self, windows: list[SlidingWindow]):
        self.windows = windows

    def check(self) -> LimiterResult:
        """All windows must allow. Returns the most restrictive result."""
        worst = None
        for w in self.windows:
            r = w.check()
            if not r.allowed:
                worst = r
        return worst or self.windows[0].check()

    def stats(self) -> dict:
        return {w.name: w.stats() for w in self.windows}


# ─── Token bucket variant ─────────────────────────────────────────────────────

class TokenBucket:
    """
    Token bucket rate limiter. Tokens refill at `rate` per second,
    up to `capacity`. Each check() consumes one token.

    Better for smoothing out bursts over time vs. sliding window's
    hard per-window limit.
    """

    def __init__(self, capacity: int, refill_rate: float, *, name: str = "bucket"):
        self.capacity = capacity
        self.refill_rate = refill_rate  # tokens per second
        self.name = name
        self._tokens = float(capacity)
        self._last_refill = time.monotonic()
        self._lock = threading.Lock()
        self._stats = WindowStats()

    def _refill(self, now: float):
        elapsed = now - self._last_refill
        self._tokens = min(self.capacity, self._tokens + elapsed * self.refill_rate)
        self._last_refill = now

    def check(self) -> bool:
        with self._lock:
            now = time.monotonic()
            self._refill(now)
            if self._tokens >= 1:
                self._tokens -= 1
                self._stats.hits += 1
                self._stats.allows += 1
                return True
            self._stats.hits += 1
            self._stats.denies += 1
            return False

    def wait_for_token(self) -> float:
        """Block until a token is available. Returns wait time in ms."""
        wait_ms = 0.0
        while not self.check():
            needed = 1 - self._tokens
            wait_sec = needed / self.refill_rate
            wait_ms += wait_sec * 1000
            time.sleep(wait_sec)
        return wait_ms

    def stats(self) -> dict:
        with self._lock:
            now = time.monotonic()
            self._refill(now)
            s = self._stats
            return {
                "name": self.name,
                "tokens_available": round(self._tokens, 2),
                "capacity": self.capacity,
                "refill_rate": self.refill_rate,
                "hits": s.hits,
                "allows": s.allows,
                "denies": s.denies,
                "hit_rate": round(s.hit_rate() * 100, 2),
            }


# ─── Persisted limiter (file-backed) ─────────────────────────────────────────

class PersistedSlidingWindow(SlidingWindow):
    """
    SlidingWindow that saves timestamps to a JSON file after every mutation.
    Survives process restarts. Uses write-temp + rename for atomicity.
    """

    def __init__(self, limit: int, window: float, path: str, **kwargs):
        super().__init__(limit, window, **kwargs)
        self.path = Path(path)
        self._load()

    def _load(self):
        if self.path.exists():
            try:
                data = json.loads(self.path.read_text())
                self._timestamps = data.get("timestamps", [])
                # Filter out expired on load
                now = time.monotonic()
                self._timestamps = [t for t in self._timestamps if now - t < self.window]
            except (json.JSONDecodeError, OSError):
                self._timestamps = []

    def _save(self):
        try:
            tmp = self.path.with_suffix(".tmp")
            tmp.write_text(json.dumps({"timestamps": self._timestamps}))
            tmp.rename(self.path)
        except OSError:
            pass  # Non-fatal if persistence fails

    def check(self) -> LimiterResult:
        with self._lock:
            result = super().check()
            self._save()
            return result

    def reset(self):
        with self._lock:
            super().reset()
            self._save()


# ─── CLI ──────────────────────────────────────────────────────────────────────

def cmd_limiter(args):
    limiter = SlidingWindow(limit=args.limit, window=args.window, name=args.name)

    print(f"[tiny-sliding-window] {args.name}: {args.limit} req / {args.window}s")
    print(f"[{time.strftime('%H:%M:%S')}] Ready. Press Ctrl+C to stop and show stats.")
    print()

    try:
        while True:
            result = limiter.check()
            ts = time.strftime("%H:%M:%S")
            if result.allowed:
                print(f"[{ts}] ✅ allowed ({result.current_hits}/{result.limit})")
            else:
                print(f"[{ts}] ❌ denied — wait {result.wait_ms:.0f}ms")
                time.sleep(result.wait_ms / 1000)
            time.sleep(args.interval)
    except KeyboardInterrupt:
        print()
        print(json.dumps(limiter.stats(), indent=2))


def cmd_bucket(args):
    bucket = TokenBucket(capacity=args.capacity, refill_rate=args.rate, name=args.name)
    print(f"[tiny-sliding-window] TokenBucket: {args.capacity} tokens, refill {args.rate}/s")
    try:
        while True:
            if bucket.check():
                print(f"[{time.strftime('%H:%M:%S')}] ✅ token acquired ({bucket._tokens:.1f} remaining)")
            else:
                print(f"[{time.strftime('%H:%M:%S')}] ❌ no token — waiting...")
                bucket.wait_for_token()
            time.sleep(args.interval)
    except KeyboardInterrupt:
        print()
        print(json.dumps(bucket.stats(), indent=2))


def cmd_stats(args):
    limiter = PersistedSlidingWindow(limit=60, window=60.0, path=args.path)
    print(json.dumps(limiter.stats(), indent=2))


def main():
    parser = argparse.ArgumentParser(description="tiny-sliding-window — Sliding window rate limiter")
    sub = parser.add_subparsers(dest="cmd")

    lim = sub.add_parser("limiter", help="Run interactive limiter demo")
    lim.add_argument("--limit", "-l", type=int, default=10, help="Max requests per window")
    lim.add_argument("--window", "-w", type=float, default=60.0, help="Window size in seconds")
    lim.add_argument("--interval", "-i", type=float, default=1.0, help="Check interval")
    lim.add_argument("--name", "-n", default="default")
    lim.set_defaults(func=cmd_limiter)

    bkt = sub.add_parser("bucket", help="Run token bucket demo")
    bkt.add_argument("--capacity", "-c", type=int, default=5, help="Bucket capacity")
    bkt.add_argument("--rate", "-r", type=float, default=1.0, help="Refill rate (tokens/s)")
    bkt.add_argument("--interval", "-i", type=float, default=0.5)
    bkt.add_argument("--name", "-n", default="bucket")
    bkt.set_defaults(func=cmd_bucket)

    stt = sub.add_parser("stats", help="Show stats for persisted limiter")
    stt.add_argument("path", help="Path to persisted JSON file")
    stt.set_defaults(func=cmd_stats)

    args = parser.parse_args()
    if not args.cmd:
        parser.print_help()
        sys.exit(1)
    args.func(args)


if __name__ == "__main__":
    main()
