#!/usr/bin/env python3
"""
Backfill is_empty for historical ticks by querying Qubic RPC.

For each tick in qubic.ticks, asks the public Qubic RPC for tick-data and
records whether the tick was empty (skipped at the protocol level).

A tick is considered EMPTY when ALL of these match the indexer's definition
(IsEmpty = HasNoTickData || IsSkipped):
  - RPC returns HTTP 404 (no tick data archived), OR
  - tickData.signatureHex is all zeros / missing (computor didn't sign), OR
  - response is otherwise empty.

A tick is considered NON-EMPTY when:
  - RPC returns 200 with tickData containing a real (non-zero) signatureHex.

Anything else (timeouts, 5xx, malformed response) is recorded as "unknown"
and retried on the next run — the script is fully resumable.

Strategy:
  1. Stage results in qubic.tick_empty_backfill (tick_number, is_empty, checked_at)
  2. Process ticks the staging table doesn't yet have
  3. Apply once verified: ALTER TABLE qubic.ticks UPDATE is_empty = ...

This script ONLY writes to the staging table — it never modifies qubic.ticks.
At the end it prints the SQL to apply, which you run manually after spot-checking.

Usage:
    # Dry-run preview (just print plan)
    python3 backfill_is_empty.py --plan

    # Run the probe (resumable; rerun if it dies)
    python3 backfill_is_empty.py --rpc https://rpc.qubic.org \
        --clickhouse http://localhost:8123 --concurrency 32

    # After the probe completes, verify and apply (separate manual step):
    python3 backfill_is_empty.py --print-apply-sql
"""

import argparse
import http.client
import json
import os
import signal
import socket
import ssl
import sys
import threading
import time
import urllib.parse
import urllib.request
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Optional


DEFAULT_RPC = "https://rpc.qubic.org"
DEFAULT_CH = os.environ.get("CLICKHOUSE_URL", "http://localhost:8123")
DEFAULT_DB = "qubic"

USER_AGENT = "qubic-explorer-empty-tick-backfill"

# State for clean Ctrl-C
_stop_flag = threading.Event()


# ---------------------------------------------------------------------------
# RPC layer (per-thread keep-alive HTTPS connection)
# ---------------------------------------------------------------------------
_tls = threading.local()


def _get_rpc_conn(rpc_url: str) -> http.client.HTTPConnection:
    parsed = urllib.parse.urlparse(rpc_url)
    host = parsed.netloc
    cls = http.client.HTTPSConnection if parsed.scheme == "https" else http.client.HTTPConnection
    conns = getattr(_tls, "rpc_conns", None)
    if conns is None:
        conns = {}
        _tls.rpc_conns = conns
    conn = conns.get(host)
    if conn is None:
        conn = cls(host, timeout=20)
        conns[host] = conn
    return conn


def _drop_rpc_conn(rpc_url: str) -> None:
    host = urllib.parse.urlparse(rpc_url).netloc
    conns = getattr(_tls, "rpc_conns", {})
    c = conns.pop(host, None)
    if c:
        try:
            c.close()
        except Exception:
            pass


def probe_tick(rpc_url: str, tick: int) -> Optional[bool]:
    """
    Return True if tick is empty, False if non-empty, None if unknown (retry later).
    """
    parsed = urllib.parse.urlparse(rpc_url)
    path = parsed.path.rstrip("/") + f"/v1/ticks/{tick}/tick-data"
    headers = {
        "User-Agent": USER_AGENT,
        "Accept": "application/json",
        "Connection": "keep-alive",
    }

    last_err: Optional[str] = None
    for attempt in range(2):
        conn = _get_rpc_conn(rpc_url)
        try:
            conn.request("GET", path, headers=headers)
            resp = conn.getresponse()
            status = resp.status
            body = resp.read()
        except (http.client.HTTPException, OSError) as e:
            last_err = f"transport: {e}"
            _drop_rpc_conn(rpc_url)
            if attempt == 0:
                continue
            return None

        if status == 404:
            # No tick data archived — tick is empty
            return True

        if status == 200:
            try:
                data = json.loads(body)
            except json.JSONDecodeError:
                last_err = "json decode"
                if attempt == 0:
                    continue
                return None

            td = data.get("tickData")
            if td is None or (isinstance(td, dict) and not td):
                return True

            # tickData present — check signature
            sig = td.get("signatureHex") or td.get("signature") or ""
            if not sig or all(ch == "0" for ch in sig):
                return True

            return False

        # 5xx / 429 / other — transient
        last_err = f"HTTP {status}"
        if status == 429:
            time.sleep(1.0)
        if attempt == 0:
            continue
        return None

    return None


# ---------------------------------------------------------------------------
# ClickHouse layer (HTTP interface)
# ---------------------------------------------------------------------------

def ch_query(ch_url: str, db: str, sql: str, body: bytes | None = None) -> bytes:
    """Run a query against ClickHouse HTTP. Returns raw response bytes."""
    parsed = urllib.parse.urlparse(ch_url)
    base = f"{parsed.scheme}://{parsed.netloc}"
    qs = urllib.parse.urlencode({"database": db, "query": sql})
    url = f"{base}/?{qs}"
    req = urllib.request.Request(
        url,
        data=body,
        method="POST",
        headers={"User-Agent": USER_AGENT},
    )
    with urllib.request.urlopen(req, timeout=120) as resp:
        return resp.read()


def ensure_staging_table(ch_url: str, db: str) -> None:
    sql = (
        f"CREATE TABLE IF NOT EXISTS {db}.tick_empty_backfill ("
        "  tick_number UInt64,"
        "  is_empty UInt8,"
        "  checked_at DateTime64(3) DEFAULT now64(3)"
        ") ENGINE = ReplacingMergeTree(checked_at) "
        "ORDER BY tick_number"
    )
    ch_query(ch_url, db, sql)


def get_tick_range(ch_url: str, db: str) -> tuple[int, int]:
    """Get min/max tick_number from qubic.ticks."""
    raw = ch_query(ch_url, db, f"SELECT min(tick_number), max(tick_number) FROM {db}.ticks FORMAT TabSeparated")
    parts = raw.decode().strip().split("\t")
    return int(parts[0]), int(parts[1])


def get_already_done(ch_url: str, db: str) -> set[int]:
    """Get the set of ticks we've already probed (from staging table)."""
    raw = ch_query(ch_url, db, f"SELECT tick_number FROM {db}.tick_empty_backfill FINAL FORMAT TabSeparated")
    if not raw.strip():
        return set()
    return {int(line) for line in raw.decode().splitlines() if line.strip()}


def get_all_tick_numbers(ch_url: str, db: str) -> list[int]:
    """Stream all tick_number values from qubic.ticks. Sorted ascending."""
    raw = ch_query(ch_url, db, f"SELECT tick_number FROM {db}.ticks ORDER BY tick_number FORMAT TabSeparated")
    return [int(line) for line in raw.decode().splitlines() if line.strip()]


def insert_batch(ch_url: str, db: str, rows: list[tuple[int, int]]) -> None:
    """Insert (tick_number, is_empty) pairs into staging."""
    if not rows:
        return
    body = "\n".join(f"{t}\t{e}" for t, e in rows).encode()
    sql = f"INSERT INTO {db}.tick_empty_backfill (tick_number, is_empty) FORMAT TabSeparated"
    ch_query(ch_url, db, sql, body=body)


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> int:
    p = argparse.ArgumentParser(description="Backfill is_empty for historical ticks via Qubic RPC")
    p.add_argument("--rpc", default=DEFAULT_RPC, help=f"Qubic RPC base URL (default: {DEFAULT_RPC})")
    p.add_argument("--clickhouse", default=DEFAULT_CH, help=f"ClickHouse HTTP URL (default: {DEFAULT_CH})")
    p.add_argument("--db", default=DEFAULT_DB, help=f"Database (default: {DEFAULT_DB})")
    p.add_argument("--concurrency", type=int, default=32, help="Parallel RPC requests (default: 32)")
    p.add_argument("--from-tick", type=int, default=None, help="Limit lower bound (defaults to min tick in ticks)")
    p.add_argument("--to-tick", type=int, default=None, help="Limit upper bound (defaults to max tick in ticks)")
    p.add_argument("--batch-size", type=int, default=2000, help="Staging insert batch size (default: 2000)")
    p.add_argument("--plan", action="store_true", help="Show plan and exit")
    p.add_argument("--print-apply-sql", action="store_true", help="Print the final UPDATE SQL and exit")
    args = p.parse_args()

    if args.print_apply_sql:
        print(_apply_sql(args.db))
        return 0

    print(f"[setup] ensuring staging table {args.db}.tick_empty_backfill")
    ensure_staging_table(args.clickhouse, args.db)

    print(f"[setup] reading tick range from {args.db}.ticks")
    min_t, max_t = get_tick_range(args.clickhouse, args.db)
    print(f"        ticks span: {min_t} .. {max_t}  ({max_t - min_t + 1:,} ticks)")

    lo = args.from_tick if args.from_tick is not None else min_t
    hi = args.to_tick if args.to_tick is not None else max_t
    print(f"        target range: {lo} .. {hi}")

    print(f"[setup] reading list of ticks already in {args.db}.ticks ...")
    all_ticks = get_all_tick_numbers(args.clickhouse, args.db)
    all_ticks = [t for t in all_ticks if lo <= t <= hi]
    print(f"        {len(all_ticks):,} ticks in scope")

    print(f"[setup] reading already-probed ticks from staging ...")
    done = get_already_done(args.clickhouse, args.db)
    todo = [t for t in all_ticks if t not in done]
    print(f"        already done: {len(done):,}    todo: {len(todo):,}")

    if args.plan:
        return 0

    if not todo:
        print("[done] nothing to probe")
        print()
        print("To apply the backfill, run:")
        print(_apply_sql(args.db))
        return 0

    # Wire up Ctrl-C
    def _on_signal(signum, frame):
        print("\n[interrupt] flushing pending batch and exiting cleanly...", file=sys.stderr)
        _stop_flag.set()
    signal.signal(signal.SIGINT, _on_signal)
    signal.signal(signal.SIGTERM, _on_signal)

    # Parallel probe
    print(f"[probe] starting with concurrency={args.concurrency}")
    start = time.time()
    completed = 0
    n_empty = 0
    n_filled = 0
    n_unknown = 0
    pending: list[tuple[int, int]] = []
    pending_lock = threading.Lock()
    progress_t = time.time()

    def flush():
        nonlocal pending
        with pending_lock:
            if pending:
                batch = pending
                pending = []
            else:
                batch = []
        if batch:
            insert_batch(args.clickhouse, args.db, batch)

    with ThreadPoolExecutor(max_workers=args.concurrency) as pool:
        future_to_tick = {pool.submit(probe_tick, args.rpc, t): t for t in todo}
        try:
            for fut in as_completed(future_to_tick):
                if _stop_flag.is_set():
                    break
                tick = future_to_tick[fut]
                completed += 1
                try:
                    res = fut.result()
                except Exception:
                    res = None
                if res is True:
                    n_empty += 1
                    with pending_lock:
                        pending.append((tick, 1))
                elif res is False:
                    n_filled += 1
                    with pending_lock:
                        pending.append((tick, 0))
                else:
                    n_unknown += 1

                if len(pending) >= args.batch_size:
                    flush()

                now = time.time()
                if now - progress_t >= 5.0:
                    elapsed = now - start
                    rate = completed / max(elapsed, 0.001)
                    eta = (len(todo) - completed) / max(rate, 0.001)
                    pct = 100.0 * completed / len(todo)
                    print(
                        f"[probe] {completed:,}/{len(todo):,} ({pct:.1f}%)  "
                        f"empty={n_empty:,} filled={n_filled:,} unknown={n_unknown:,}  "
                        f"{rate:.0f}/s  eta={int(eta//60)}m{int(eta%60)}s"
                    )
                    progress_t = now
        finally:
            flush()

    elapsed = time.time() - start
    print()
    print(f"[done] probed {completed:,} ticks in {int(elapsed)}s")
    print(f"       empty={n_empty:,}   non-empty={n_filled:,}   unknown={n_unknown:,}")
    if n_unknown:
        print(f"       NOTE: {n_unknown} ticks couldn't be determined — rerun this script to retry them.")
    print()
    print("To apply the backfill, review the staging table then run:")
    print(_apply_sql(args.db))
    return 0


def _apply_sql(db: str) -> str:
    return f"""
-- Verify staging counts make sense:
SELECT is_empty, count() FROM {db}.tick_empty_backfill FINAL GROUP BY is_empty;

-- Spot-check a sample of empty ticks against an explorer or RPC manually.

-- Apply: only update ticks where is_empty would change (current = 0, staged = 1):
ALTER TABLE {db}.ticks
UPDATE is_empty = 1
WHERE tick_number IN (
    SELECT tick_number FROM {db}.tick_empty_backfill FINAL WHERE is_empty = 1
);

-- Watch the mutation finish:
SELECT command, parts_to_do, is_done FROM system.mutations
WHERE database='{db}' AND table='ticks' AND NOT is_done;

-- Once done, verify:
SELECT countIf(is_empty=1) AS empty_ticks, count() AS total FROM {db}.ticks;

-- Optional: drop staging table to reclaim disk
-- DROP TABLE {db}.tick_empty_backfill;
""".strip()


if __name__ == "__main__":
    sys.exit(main())
