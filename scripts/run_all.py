"""
Launch the full SmartQueue system with a single command:

    python scripts/run_all.py

Starts three processes:
  1. Producer  — FastAPI app via uvicorn on port 8000
  2. Worker    — SQS polling loop
  3. Scaler    — auto-scaler + circuit breaker

All three write to stdout/stderr. Ctrl-C shuts them all down cleanly.
"""

import os
import signal
import subprocess
import sys
import time

# Project root is one level above this script
ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

PYTHON = sys.executable  # same interpreter that's running this script

PROCESSES = [
    {
        "name": "producer",
        "cmd": [PYTHON, "-m", "uvicorn", "producer.main:app", "--host", "0.0.0.0", "--port", "8000"],
    },
    {
        "name": "worker",
        "cmd": [PYTHON, "worker/main.py"],
    },
    {
        "name": "scaler",
        "cmd": [PYTHON, "scaler/main.py"],
    },
]


def start_process(spec: dict) -> subprocess.Popen:
    proc = subprocess.Popen(
        spec["cmd"],
        cwd=ROOT,
        stdout=sys.stdout,
        stderr=sys.stderr,
    )
    print(f"[run_all] Started {spec['name']}  pid={proc.pid}", flush=True)
    return proc


def shutdown(procs: list[subprocess.Popen]) -> None:
    print("\n[run_all] Shutting down all processes ...", flush=True)
    for proc in procs:
        if proc.poll() is None:
            proc.send_signal(signal.SIGINT)

    deadline = time.time() + 5
    for proc in procs:
        remaining = max(0.0, deadline - time.time())
        try:
            proc.wait(timeout=remaining)
        except subprocess.TimeoutExpired:
            proc.kill()

    print("[run_all] All processes stopped.", flush=True)


def main() -> None:
    procs: list[subprocess.Popen] = []

    try:
        for spec in PROCESSES:
            procs.append(start_process(spec))
            time.sleep(1)  # small stagger so logs are easier to read at startup

        print("[run_all] System running. Press Ctrl-C to stop.\n", flush=True)

        # Monitor: restart any process that exits unexpectedly
        while True:
            for i, (spec, proc) in enumerate(zip(PROCESSES, procs)):
                if proc.poll() is not None:
                    print(
                        f"[run_all] {spec['name']} exited (rc={proc.returncode}), restarting ...",
                        flush=True,
                    )
                    procs[i] = start_process(spec)
            time.sleep(5)

    except KeyboardInterrupt:
        pass
    finally:
        shutdown(procs)


if __name__ == "__main__":
    main()
