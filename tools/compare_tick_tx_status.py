#!/usr/bin/env python3
"""
Compare transaction execution status between Qubic RPC and our Explorer.

For each tick in [from_tick, to_tick], fetches transactions from both sources
and lists transactions where moneyFlew (RPC) != executed (Explorer).

Usage:
    python3 compare_tick_tx_status.py <from_tick> <to_tick> [--explorer URL] [--rpc URL]

Example:
    python3 compare_tick_tx_status.py 46310000 46310100
    python3 compare_tick_tx_status.py 46310000 46310100 --explorer https://explorer-demo.qubic.tools
"""
import argparse
import http.client
import json
import sys
import threading
import time
import urllib.parse
from concurrent.futures import ThreadPoolExecutor, as_completed


DEFAULT_RPC = "https://rpc.qubic.org/query/v1"
DEFAULT_EXPLORER = "https://explorer-demo.qubic.tools"

USER_AGENT = "Mozilla/5.0 (qubic-explorer-compare-tool)"

# Per-thread persistent HTTPS connections (keep-alive, avoids TLS handshake per request)
_tls = threading.local()


def _get_conn(url: str) -> http.client.HTTPSConnection:
    """Get a cached HTTPS connection for this thread + host."""
    parsed = urllib.parse.urlparse(url)
    host = parsed.netloc
    conns = getattr(_tls, "conns", None)
    if conns is None:
        conns = {}
        _tls.conns = conns
    conn = conns.get(host)
    if conn is None:
        conn = http.client.HTTPSConnection(host, timeout=30)
        conns[host] = conn
    return conn


def _request(method: str, url: str, body: bytes | None = None) -> tuple[int, bytes]:
    """Make an HTTPS request using keep-alive. Retries once on connection error."""
    parsed = urllib.parse.urlparse(url)
    path = parsed.path + ("?" + parsed.query if parsed.query else "")
    headers = {
        "User-Agent": USER_AGENT,
        "Accept": "application/json",
        "Connection": "keep-alive",
    }
    if body is not None:
        headers["Content-Type"] = "application/json"
        headers["Content-Length"] = str(len(body))

    for attempt in range(2):
        conn = _get_conn(url)
        try:
            conn.request(method, path, body=body, headers=headers)
            resp = conn.getresponse()
            data = resp.read()
            return resp.status, data
        except (http.client.HTTPException, OSError):
            # Reset the connection and retry once
            try:
                conn.close()
            except Exception:
                pass
            _tls.conns.pop(parsed.netloc, None)
            if attempt == 1:
                raise
    return 0, b""


def fetch_rpc_tick(rpc_url: str, tick: int) -> dict[str, tuple[bool, int]]:
    """Return { txHash: (moneyFlew, amount) } for all QU transfers in a tick."""
    url = f"{rpc_url}/getTransactionsForTick"
    body = json.dumps({"tickNumber": tick}).encode()
    status, raw = _request("POST", url, body)
    if status == 404:
        return {}
    if status >= 400:
        raise RuntimeError(f"RPC HTTP {status}")
    data = json.loads(raw)

    # Response may be a dict with "transactions" key, or a list directly.
    if isinstance(data, dict):
        tx_list = data.get("transactions") or []
    elif isinstance(data, list):
        tx_list = data
    else:
        tx_list = []

    result = {}
    for tx in tx_list:
        if not isinstance(tx, dict):
            continue
        # Some responses wrap transaction in { "transaction": {...}, "moneyFlew": ... }
        inner = tx.get("transaction") if "transaction" in tx else tx
        if not isinstance(inner, dict):
            inner = tx
        h = inner.get("hash") or tx.get("hash") or tx.get("txId")
        if not h:
            continue

        # Only compare QU transfers: inputType == 0 and amount > 0
        input_type = inner.get("inputType", tx.get("inputType", 0))
        try:
            input_type = int(input_type)
        except (TypeError, ValueError):
            input_type = 0
        if input_type != 0:
            continue

        amount = inner.get("amount", tx.get("amount", 0))
        try:
            amount = int(amount)
        except (TypeError, ValueError):
            amount = 0
        if amount <= 0:
            continue

        money_flew = tx.get("moneyFlew")
        if money_flew is None:
            money_flew = inner.get("moneyFlew")
        result[h] = (bool(money_flew), amount)
    return result


def fetch_explorer_tick(explorer_url: str, tick: int) -> dict[str, tuple[bool, int]]:
    """Return { txHash: (executed, amount) } for all QU transfers in a tick."""
    result: dict[str, tuple[bool, int]] = {}
    page = 1
    while True:
        url = f"{explorer_url}/api/ticks/{tick}/transactions?page={page}&limit=1024&skipCount=true"
        status, raw = _request("GET", url)
        if status == 404:
            return {}
        if status >= 400:
            raise RuntimeError(f"Explorer HTTP {status}")
        data = json.loads(raw)

        for tx in data.get("items") or []:
            h = tx.get("hash")
            if not h:
                continue
            # Only QU transfers: inputType == 0 and amount > 0
            input_type = tx.get("inputType", 0)
            try:
                input_type = int(input_type)
            except (TypeError, ValueError):
                input_type = 0
            if input_type != 0:
                continue
            amount = tx.get("amount", 0)
            try:
                amount = int(amount)
            except (TypeError, ValueError):
                amount = 0
            if amount <= 0:
                continue
            result[h] = (bool(tx.get("executed", False)), amount)

        if not data.get("hasNextPage"):
            break
        page += 1
    return result


def fetch_both(tick: int, rpc_url: str, explorer_url: str):
    """Fetch a tick from both sources. Returns (tick, rpc_dict, exp_dict, rpc_ms, exp_ms, error)."""
    try:
        t0 = time.monotonic()
        rpc = fetch_rpc_tick(rpc_url, tick)
        t1 = time.monotonic()
        exp = fetch_explorer_tick(explorer_url, tick)
        t2 = time.monotonic()
        return (tick, rpc, exp, (t1 - t0) * 1000, (t2 - t1) * 1000, None)
    except Exception as e:
        return (tick, None, None, 0.0, 0.0, str(e))


def compare(from_tick: int, to_tick: int, rpc_url: str, explorer_url: str, workers: int, output_path: str) -> int:
    total_ticks = 0
    total_tx = 0
    mismatches: list[dict] = []
    missing_in_rpc: list[tuple[int, str]] = []
    missing_in_explorer: list[tuple[int, str]] = []

    # Direction counters
    rpc_true_exp_false = 0  # RPC says executed, Explorer says not
    rpc_false_exp_true = 0  # RPC says not executed, Explorer says yes

    total_range = to_tick - from_tick + 1
    start_time = time.monotonic()
    samples_printed = 0
    MAX_INLINE_SAMPLES = 10

    # Windowed rate tracking (last 1000 ticks)
    from collections import deque
    recent_times: deque[float] = deque(maxlen=1000)
    recent_rpc_ms: deque[float] = deque(maxlen=1000)
    recent_exp_ms: deque[float] = deque(maxlen=1000)

    # Open output file and write header immediately so user can tail it
    out_fh = open(output_path, "w", buffering=1)  # line-buffered
    out_fh.write(f"# Qubic tx status comparison: ticks {from_tick}..{to_tick}\n")
    out_fh.write(f"# RPC: {rpc_url}\n")
    out_fh.write(f"# Explorer: {explorer_url}\n")
    out_fh.write(f"# Started: {time.strftime('%Y-%m-%d %H:%M:%S')}\n")
    out_fh.write("#\n")
    out_fh.write("# kind,tick,rpc_moneyFlew,explorer_executed,hash\n")

    print(f"Writing results to: {output_path}", file=sys.stderr)
    header = f"{'tick':>10} | {'rpc':>3} | {'exp':>3} | hash"
    sep = "-" * 90
    print(sep, file=sys.stderr)
    print(header, file=sys.stderr)
    print(sep, file=sys.stderr)

    with ThreadPoolExecutor(max_workers=workers) as pool:
        futures = [
            pool.submit(fetch_both, t, rpc_url, explorer_url)
            for t in range(from_tick, to_tick + 1)
        ]

        for fut in as_completed(futures):
            tick, rpc, exp, rpc_ms, exp_ms, err = fut.result()
            total_ticks += 1
            recent_times.append(time.monotonic())
            if err is None:
                recent_rpc_ms.append(rpc_ms)
                recent_exp_ms.append(exp_ms)

            if err is not None:
                print(f"  [tick {tick}] ERROR: {err}", file=sys.stderr)
                continue

            total_tx += len(set(rpc) | set(exp))

            for h, (rpc_status, rpc_amount) in rpc.items():
                if h not in exp:
                    missing_in_explorer.append((tick, h))
                    out_fh.write(f"missing_in_explorer,{tick},{rpc_status},,{h}\n")
                    continue
                exp_status, _exp_amount = exp[h]
                if rpc_status != exp_status:
                    # Spam filter: if RPC says it flew and Explorer says failed,
                    # and amount < 100, this is a bobs-spam victim — ignore.
                    if rpc_status and not exp_status and rpc_amount < 100:
                        continue

                    mismatches.append({
                        "tick": tick,
                        "hash": h,
                        "rpc_moneyFlew": rpc_status,
                        "explorer_executed": exp_status,
                    })
                    if rpc_status and not exp_status:
                        rpc_true_exp_false += 1
                    else:
                        rpc_false_exp_true += 1

                    out_fh.write(f"mismatch,{tick},{rpc_status},{exp_status},{h}\n")

                    if samples_printed < MAX_INLINE_SAMPLES:
                        print(
                            f"{tick:>10} | {'Y' if rpc_status else 'N':>3} | "
                            f"{'Y' if exp_status else 'N':>3} | {h}",
                            file=sys.stderr,
                        )
                        samples_printed += 1
                        if samples_printed == MAX_INLINE_SAMPLES:
                            print(f"... (further mismatches written to {output_path})", file=sys.stderr)

            for h in exp:
                if h not in rpc:
                    missing_in_rpc.append((tick, h))
                    exp_status, _ = exp[h]
                    out_fh.write(f"missing_in_rpc,{tick},,{exp_status},{h}\n")

            if total_ticks % 100 == 0 or total_ticks == total_range:
                elapsed = time.monotonic() - start_time
                avg_rate = total_ticks / elapsed if elapsed > 0 else 0

                # Windowed (current) rate over last N ticks
                if len(recent_times) >= 2:
                    window_secs = recent_times[-1] - recent_times[0]
                    cur_rate = (len(recent_times) - 1) / window_secs if window_secs > 0 else 0
                else:
                    cur_rate = avg_rate

                # Use current rate for ETA (more accurate if things are changing)
                eta_rate = cur_rate if cur_rate > 0 else avg_rate
                remaining = (total_range - total_ticks) / eta_rate if eta_rate > 0 else 0
                pct = 100.0 * total_ticks / total_range
                avg_rpc_ms = sum(recent_rpc_ms) / len(recent_rpc_ms) if recent_rpc_ms else 0
                avg_exp_ms = sum(recent_exp_ms) / len(recent_exp_ms) if recent_exp_ms else 0
                print(
                    f"  [{pct:5.1f}%] {total_ticks}/{total_range} | "
                    f"cur={cur_rate:.1f}/s avg={avg_rate:.1f}/s | "
                    f"rpc={avg_rpc_ms:.0f}ms exp={avg_exp_ms:.0f}ms | "
                    f"ETA {remaining/60:.1f}m | "
                    f"mismatches={len(mismatches)} "
                    f"(rpc=Y,exp=N: {rpc_true_exp_false}, rpc=N,exp=Y: {rpc_false_exp_true}) | "
                    f"miss_rpc={len(missing_in_rpc)} miss_exp={len(missing_in_explorer)}",
                    file=sys.stderr,
                )

    summary_lines = [
        "=" * 90,
        f"Scanned ticks:            {total_ticks}",
        f"Total unique transactions: {total_tx}",
        f"Mismatches total:         {len(mismatches)}",
        f"  RPC=executed,   Explorer=not: {rpc_true_exp_false}",
        f"  RPC=not,        Explorer=executed: {rpc_false_exp_true}",
        f"Missing in RPC:           {len(missing_in_rpc)}",
        f"Missing in Explorer:      {len(missing_in_explorer)}",
        "=" * 90,
    ]
    print()
    for line in summary_lines:
        print(line)
    print()

    out_fh.write("#\n")
    out_fh.write(f"# Finished: {time.strftime('%Y-%m-%d %H:%M:%S')}\n")
    for line in summary_lines:
        out_fh.write(f"# {line}\n")
    out_fh.close()
    print(f"Full results written to: {output_path}")
    print()

    return 0 if not mismatches else 1


def main():
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("from_tick", type=int)
    p.add_argument("to_tick", type=int)
    p.add_argument("--rpc", default=DEFAULT_RPC, help=f"Qubic RPC base URL (default: {DEFAULT_RPC})")
    p.add_argument("--explorer", default=DEFAULT_EXPLORER, help=f"Explorer base URL (default: {DEFAULT_EXPLORER})")
    p.add_argument("--workers", type=int, default=32, help="Parallel HTTP workers (default: 32)")
    p.add_argument("--output", "-o", default=None, help="Output file path (default: compare_<from>_<to>.csv)")
    args = p.parse_args()

    if args.output is None:
        args.output = f"compare_{args.from_tick}_{args.to_tick}.csv"

    if args.to_tick < args.from_tick:
        p.error("to_tick must be >= from_tick")

    print(f"Comparing ticks {args.from_tick}..{args.to_tick}")
    print(f"  RPC:      {args.rpc}")
    print(f"  Explorer: {args.explorer}")
    print(f"  Workers:  {args.workers}")
    print()

    sys.exit(compare(args.from_tick, args.to_tick, args.rpc, args.explorer, args.workers, args.output))


if __name__ == "__main__":
    main()
