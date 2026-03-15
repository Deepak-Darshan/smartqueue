import json
import logging
import os
import random
import sys
import time

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from shared.config import get_sqs_client, QUEUES
from shared.db import update_task_status

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%S",
)
log = logging.getLogger(__name__)

PRIORITY_ORDER = ["high", "normal", "low"]
EMPTY_QUEUE_WAIT = 2        # seconds to sleep when all queues are empty
SIMULATED_FAILURE_RATE = 0.2  # 20% random failure rate to exercise retry logic


def process_task(task: dict) -> None:
    """Simulate task execution. Raises on simulated failure."""
    log.info(
        f"[{task['task_id']}] Processing  task_type={task['task_type']}  "
        f"payload={task['payload']}"
    )
    time.sleep(1)  # simulated work

    if random.random() < SIMULATED_FAILURE_RATE:
        raise RuntimeError("simulated processing failure")


def _try_update_status(task_id: str, status: str, **kwargs) -> None:
    """Call update_task_status, logging and swallowing any DynamoDB error.

    SQS message processing must never abort because of a DynamoDB outage.
    """
    try:
        update_task_status(task_id, status, **kwargs)
    except Exception as exc:
        log.warning(f"[{task_id}] DynamoDB status update failed (status={status}): {exc}")


def send_to_dlq(sqs, task: dict) -> None:
    sqs.send_message(QueueUrl=QUEUES["dlq"], MessageBody=json.dumps(task))
    _try_update_status(task["task_id"], "dead_lettered", error="exceeded max retries")
    log.warning(
        f"[{task['task_id']}] DEAD-LETTERED  "
        f"attempts={task['attempts']}  max_retries={task.get('max_retries', 3)}"
    )


def requeue_task(sqs, task: dict, queue_url: str, priority: str) -> None:
    attempt = task["attempts"]
    delay = min(2 ** attempt, 900)  # SQS DelaySeconds ceiling is 900
    sqs.send_message(
        QueueUrl=queue_url,
        MessageBody=json.dumps(task),
        DelaySeconds=delay,
    )
    _try_update_status(
        task["task_id"], "queued",
        error=f"attempt {attempt} failed, retrying in {delay}s",
    )
    log.info(
        f"[{task['task_id']}] REQUEUED  queue={priority}  "
        f"attempt={attempt}  backoff={delay}s"
    )


def handle_message(sqs, message: dict, priority: str, queue_url: str) -> None:
    receipt_handle = message["ReceiptHandle"]
    task = json.loads(message["Body"])
    task_id = task.get("task_id", "unknown")

    log.info(
        f"[{task_id}] RECEIVED  queue={priority}  "
        f"attempt={task.get('attempts', 0)}"
    )

    try:
        _try_update_status(task_id, "processing")
        process_task(task)

        # Success — remove from queue and record result
        sqs.delete_message(QueueUrl=queue_url, ReceiptHandle=receipt_handle)
        _try_update_status(task_id, "completed", result="ok")
        log.info(f"[{task_id}] SUCCESS  queue={priority}")

    except Exception as exc:
        log.error(f"[{task_id}] FAILURE  reason={exc}")
        task["attempts"] = task.get("attempts", 0) + 1

        # Always delete the original before re-routing so it isn't redelivered
        sqs.delete_message(QueueUrl=queue_url, ReceiptHandle=receipt_handle)

        if task["attempts"] < task.get("max_retries", 3):
            requeue_task(sqs, task, queue_url, priority)
        else:
            send_to_dlq(sqs, task)


def poll_once(sqs) -> bool:
    """Check queues in priority order. Returns True if a message was handled."""
    for priority in PRIORITY_ORDER:
        response = sqs.receive_message(
            QueueUrl=QUEUES[priority],
            MaxNumberOfMessages=1,
            WaitTimeSeconds=0,       # short-poll so high-priority is checked first
            AttributeNames=["All"],
            MessageAttributeNames=["All"],
        )
        messages = response.get("Messages", [])
        if messages:
            handle_message(sqs, messages[0], priority, QUEUES[priority])
            return True
    return False


def main() -> None:
    log.info("SmartQueue Worker started  priorities=%s", PRIORITY_ORDER)
    sqs = get_sqs_client()

    while True:
        try:
            if not poll_once(sqs):
                log.debug("All queues empty — sleeping %ss", EMPTY_QUEUE_WAIT)
                time.sleep(EMPTY_QUEUE_WAIT)
        except KeyboardInterrupt:
            log.info("Worker shutdown requested.")
            break
        except Exception as exc:
            log.error("Unexpected poll error: %s — retrying in %ss", exc, EMPTY_QUEUE_WAIT)
            time.sleep(EMPTY_QUEUE_WAIT)


if __name__ == "__main__":
    main()
