"""
SmartQueue Auto-Scaler

Polls SQS queue depths every 30 seconds and manages a pool of worker threads.
Implements a circuit breaker that opens when 3+ workers fail within 60 seconds.
Writes current state to SCALER_STATE_FILE so the producer API can serve it.
"""

import json
import logging
import os
import sys
import threading
import time
import uuid

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from shared.config import get_sqs_client, QUEUES, SCALER_STATE_FILE  # noqa: E402

# Import the worker's poll loop so each thread runs the real worker logic
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import worker.main as worker_module  # noqa: E402

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] [scaler] %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%S",
)
log = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

POLL_INTERVAL = 30          # seconds between scaling checks
EMPTY_QUEUE_WAIT = 2        # passed through to worker threads
CIRCUIT_MAX_FAILURES = 3    # failures needed to open circuit
CIRCUIT_WINDOW = 60         # seconds: failure window
CIRCUIT_COOLDOWN = 120      # seconds: how long circuit stays open

# depth → target worker count
SCALING_TIERS = [
    (0,   0,  1),   # depth == 0:   1 worker  (keep at least one warm)
    (1,  10,  2),   # depth 1–10:   2 workers
    (11, 50,  4),   # depth 11–50:  4 workers
    (51, float("inf"), 8),  # depth > 50:   8 workers
]


# ---------------------------------------------------------------------------
# Circuit Breaker
# ---------------------------------------------------------------------------

class CircuitBreaker:
    def __init__(self):
        self._lock = threading.Lock()
        self._failure_times: list[float] = []
        self._open_since: float | None = None

    def record_failure(self) -> bool:
        """Record a worker failure. Returns True if the circuit just opened."""
        with self._lock:
            now = time.time()
            # Evict failures outside the window
            self._failure_times = [t for t in self._failure_times if now - t < CIRCUIT_WINDOW]
            self._failure_times.append(now)

            if self._open_since is None and len(self._failure_times) >= CIRCUIT_MAX_FAILURES:
                self._open_since = now
                log.warning(
                    "CIRCUIT OPEN — %d failures in the last %ds",
                    len(self._failure_times), CIRCUIT_WINDOW,
                )
                return True
        return False

    @property
    def is_open(self) -> bool:
        with self._lock:
            if self._open_since is None:
                return False
            if time.time() - self._open_since >= CIRCUIT_COOLDOWN:
                log.info(
                    "CIRCUIT CLOSED — cooldown of %ds elapsed, resuming scaling",
                    CIRCUIT_COOLDOWN,
                )
                self._open_since = None
                self._failure_times.clear()
                return False
            return True

    @property
    def status(self) -> str:
        return "open" if self.is_open else "closed"


# ---------------------------------------------------------------------------
# Scaler
# ---------------------------------------------------------------------------

class Scaler:
    def __init__(self):
        self._sqs = get_sqs_client()
        self._lock = threading.Lock()
        self._circuit = CircuitBreaker()

        # {worker_id: {"thread": Thread, "stop": Event, "start_time": float}}
        self._workers: dict[str, dict] = {}

        # Last known state snapshot (written to file + served by API)
        self._state: dict = {
            "current_workers": 0,
            "target_workers": 1,
            "circuit_breaker_status": "closed",
            "last_scaling_action": "startup",
            "last_checked": None,
            "queue_depths": {},
        }

    # ------------------------------------------------------------------
    # Queue depth
    # ------------------------------------------------------------------

    def _get_queue_depths(self) -> dict[str, int]:
        depths: dict[str, int] = {}
        for name, url in QUEUES.items():
            if name == "dlq":
                continue
            resp = self._sqs.get_queue_attributes(
                QueueUrl=url,
                AttributeNames=["ApproximateNumberOfMessages"],
            )
            depths[name] = int(resp["Attributes"]["ApproximateNumberOfMessages"])
        return depths

    @staticmethod
    def _target_for_depth(depth: int) -> int:
        for low, high, target in SCALING_TIERS:
            if low <= depth <= high:
                return target
        return 8  # fallback

    # ------------------------------------------------------------------
    # Worker thread lifecycle
    # ------------------------------------------------------------------

    def _worker_thread_fn(self, worker_id: str, stop_event: threading.Event) -> None:
        """Entry point for each worker thread. Mirrors worker/main.py::main()."""
        log.info("Worker %s started", worker_id)
        sqs = get_sqs_client()
        try:
            while not stop_event.is_set():
                try:
                    if not worker_module.poll_once(sqs):
                        stop_event.wait(timeout=EMPTY_QUEUE_WAIT)
                except KeyboardInterrupt:
                    break
                except Exception as exc:
                    log.error("Worker %s poll error: %s", worker_id, exc)
                    stop_event.wait(timeout=EMPTY_QUEUE_WAIT)
        except Exception as exc:
            log.error("Worker %s crashed: %s", worker_id, exc)
            just_opened = self._circuit.record_failure()
            if just_opened:
                log.warning("Circuit opened due to worker crash")
        finally:
            log.info("Worker %s stopped", worker_id)
            with self._lock:
                self._workers.pop(worker_id, None)

    def _start_worker(self) -> str:
        worker_id = f"w-{uuid.uuid4().hex[:8]}"
        stop_event = threading.Event()
        thread = threading.Thread(
            target=self._worker_thread_fn,
            args=(worker_id, stop_event),
            name=worker_id,
            daemon=True,
        )
        with self._lock:
            self._workers[worker_id] = {
                "thread": thread,
                "stop": stop_event,
                "start_time": time.time(),
                "status": "running",
            }
        thread.start()
        return worker_id

    def _stop_worker(self, worker_id: str) -> None:
        with self._lock:
            entry = self._workers.get(worker_id)
        if entry:
            entry["stop"].set()
            log.info("Sent stop signal to worker %s", worker_id)

    def _cull_dead_workers(self) -> None:
        """Remove entries for threads that have already exited."""
        with self._lock:
            dead = [wid for wid, e in self._workers.items() if not e["thread"].is_alive()]
            for wid in dead:
                self._workers.pop(wid)

    # ------------------------------------------------------------------
    # State persistence
    # ------------------------------------------------------------------

    def _write_state(self, depths: dict, target: int, action: str) -> None:
        with self._lock:
            current = len(self._workers)

        snapshot = {
            "current_workers": current,
            "target_workers": target,
            "circuit_breaker_status": self._circuit.status,
            "last_scaling_action": action,
            "last_checked": time.time(),
            "queue_depths": depths,
        }
        self._state = snapshot

        # Atomic write: temp file → rename
        tmp = SCALER_STATE_FILE + ".tmp"
        with open(tmp, "w") as f:
            json.dump(snapshot, f)
        os.replace(tmp, SCALER_STATE_FILE)

    # ------------------------------------------------------------------
    # Core scaling logic
    # ------------------------------------------------------------------

    def _scale(self) -> None:
        depths = self._get_queue_depths()
        total_depth = sum(depths.values())
        target = self._target_for_depth(total_depth)

        self._cull_dead_workers()
        with self._lock:
            current = len(self._workers)
            worker_ids = list(self._workers.keys())

        # --- Circuit breaker check ---
        if self._circuit.is_open:
            action = "circuit_open"
            log.warning(
                "SCALING DECISION  ts=%.0f  depth=%d  current=%d  target=%d  action=%s",
                time.time(), total_depth, current, target, action,
            )
            self._write_state(depths, target, action)
            return

        # --- Scale up ---
        if current < target:
            to_add = target - current
            for _ in range(to_add):
                wid = self._start_worker()
                log.info("Spawned worker %s", wid)
            action = f"scale_up  added={to_add}"

        # --- Scale down ---
        elif current > target:
            to_remove = current - target
            for wid in worker_ids[:to_remove]:
                self._stop_worker(wid)
            action = f"scale_down  removed={to_remove}"

        else:
            action = "no_change"

        log.info(
            "SCALING DECISION  ts=%.0f  depth=%d  current=%d  target=%d  action=%s",
            time.time(), total_depth, current, target, action,
        )
        self._write_state(depths, target, action)

    # ------------------------------------------------------------------
    # Main loop
    # ------------------------------------------------------------------

    def run(self) -> None:
        log.info("SmartQueue Scaler started  poll_interval=%ds", POLL_INTERVAL)
        while True:
            try:
                self._scale()
            except KeyboardInterrupt:
                log.info("Scaler shutdown requested, stopping all workers ...")
                with self._lock:
                    ids = list(self._workers.keys())
                for wid in ids:
                    self._stop_worker(wid)
                break
            except Exception as exc:
                log.error("Scaler error: %s", exc)

            try:
                time.sleep(POLL_INTERVAL)
            except KeyboardInterrupt:
                log.info("Scaler shutdown requested.")
                break


def main() -> None:
    Scaler().run()


if __name__ == "__main__":
    main()
