"""
SmartQueue Load Test
--------------------
Sends 100 tasks concurrently (20 threads) to the producer API and reports
throughput, success rate, and latency.

Usage:
    python scripts/load_test.py [--url http://localhost:8000] [--tasks 100]

No external dependencies — uses only stdlib + boto3 (already in requirements).
"""

import argparse
import json
import sys
import time
import urllib.error
import urllib.request
from concurrent.futures import ThreadPoolExecutor, as_completed


# ---------------------------------------------------------------------------
# Task mix: 20 high / 60 normal / 20 low
# ---------------------------------------------------------------------------

def _build_tasks(total: int) -> list[dict]:
    tasks = []
    boundaries = [
        (0,                    total // 5,        "high"),
        (total // 5,           total - total // 5, "normal"),
        (total - total // 5,   total,              "low"),
    ]
    for i in range(total):
        for start, end, priority in boundaries:
            if start <= i < end:
                tasks.append({
                    "task_type": "load_test",
                    "payload": {"test_id": i, "timestamp": time.time()},
                    "priority": priority,
                })
                break
    return tasks


# ---------------------------------------------------------------------------
# Single-task sender
# ---------------------------------------------------------------------------

def _send_task(base_url: str, task: dict) -> tuple[bool, str]:
    """POST one task. Returns (success, error_message)."""
    url = f"{base_url}/tasks"
    data = json.dumps(task).encode()
    req = urllib.request.Request(
        url,
        data=data,
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    try:
        with urllib.request.urlopen(req, timeout=10) as resp:
            body = json.loads(resp.read())
            return ("task_id" in body and body.get("status") == "queued"), ""
    except urllib.error.HTTPError as exc:
        return False, f"HTTP {exc.code}"
    except Exception as exc:
        return False, str(exc)


# ---------------------------------------------------------------------------
# Load test runner
# ---------------------------------------------------------------------------

def run_load_test(base_url: str, total: int, threads: int) -> None:
    tasks = _build_tasks(total)

    print(f"\nConnecting to {base_url} ...")
    # Verify the server is up before hammering it
    try:
        with urllib.request.urlopen(f"{base_url}/health", timeout=5) as r:
            if r.status != 200:
                print(f"ERROR: /health returned {r.status}. Is the producer running?")
                sys.exit(1)
    except Exception as exc:
        print(f"ERROR: Cannot reach {base_url}/health — {exc}")
        print("Start the producer first:  uvicorn producer.main:app --port 8000")
        sys.exit(1)

    print(f"Sending {total} tasks with {threads} concurrent threads ...\n")

    successes = 0
    failures = 0
    errors: list[str] = []

    t_start = time.perf_counter()

    with ThreadPoolExecutor(max_workers=threads) as pool:
        futures = {pool.submit(_send_task, base_url, t): t for t in tasks}
        for future in as_completed(futures):
            ok, err = future.result()
            if ok:
                successes += 1
            else:
                failures += 1
                if err:
                    errors.append(err)

    elapsed = time.perf_counter() - t_start
    throughput = total / elapsed if elapsed > 0 else 0
    success_rate = successes / total * 100

    # ---------------------------------------------------------------------------
    # Summary table
    # ---------------------------------------------------------------------------
    w = 32
    print("=" * w)
    print(f"{'SmartQueue Load Test Results':^{w}}")
    print("=" * w)
    print(f"  {'Total tasks:':<18}{total:>8}")
    print(f"  {'Successful:':<18}{successes:>8}")
    print(f"  {'Failed:':<18}{failures:>8}")
    print(f"  {'Success rate:':<18}{success_rate:>7.1f}%")
    print(f"  {'Total time:':<18}{elapsed:>7.2f}s")
    print(f"  {'Throughput:':<18}{throughput:>6.1f} tasks/sec")
    print("=" * w)

    if errors:
        unique = list(dict.fromkeys(errors))  # deduplicate, preserve order
        print(f"\nError breakdown ({len(errors)} total):")
        for e in unique[:5]:
            print(f"  {e}")
        if len(unique) > 5:
            print(f"  ... and {len(unique) - 5} more")

    print()
    sys.exit(0 if failures == 0 else 1)


# ---------------------------------------------------------------------------
# CLI entry point
# ---------------------------------------------------------------------------

def main() -> None:
    parser = argparse.ArgumentParser(description="SmartQueue load test")
    parser.add_argument(
        "--url",
        default="http://localhost:8000",
        help="Producer base URL (default: http://localhost:8000)",
    )
    parser.add_argument(
        "--tasks",
        type=int,
        default=100,
        help="Total number of tasks to send (default: 100)",
    )
    parser.add_argument(
        "--threads",
        type=int,
        default=20,
        help="Concurrent threads (default: 20)",
    )
    args = parser.parse_args()
    run_load_test(args.url, args.tasks, args.threads)


if __name__ == "__main__":
    main()
