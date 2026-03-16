"""
Tests for the SmartQueue producer API.

All boto3 calls are mocked — no real AWS credentials or infrastructure needed.
"""
import os
import sys

# Ensure the project root is on sys.path before any local imports
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# Set dummy env vars *before* importing the app so that shared.config.QUEUES
# is populated at module-load time without requiring a real .env file.
os.environ.setdefault("AWS_REGION", "us-east-1")
os.environ.setdefault("SQS_HIGH_QUEUE", "https://sqs.us-east-1.amazonaws.com/000000000000/high")
os.environ.setdefault("SQS_NORMAL_QUEUE", "https://sqs.us-east-1.amazonaws.com/000000000000/normal")
os.environ.setdefault("SQS_LOW_QUEUE", "https://sqs.us-east-1.amazonaws.com/000000000000/low")
os.environ.setdefault("SQS_DLQ", "https://sqs.us-east-1.amazonaws.com/000000000000/dlq")

from unittest.mock import MagicMock, patch  # noqa: E402

from fastapi.testclient import TestClient  # noqa: E402

from producer.main import app  # noqa: E402

client = TestClient(app)


# ---------------------------------------------------------------------------
# Health check
# ---------------------------------------------------------------------------

def test_health_check():
    response = client.get("/health")
    assert response.status_code == 200
    assert response.json()["status"] == "ok"


# ---------------------------------------------------------------------------
# Task enqueue
# ---------------------------------------------------------------------------

def test_enqueue_valid_task():
    mock_sqs = MagicMock()
    mock_sqs.send_message.return_value = {"MessageId": "mock-message-id"}

    with patch("producer.main.get_sqs_client", return_value=mock_sqs), \
         patch("producer.main.save_task") as mock_save:

        response = client.post("/tasks", json={
            "task_type": "send_email",
            "payload": {"to": "user@example.com", "subject": "Hello"},
            "priority": "normal",
        })

    assert response.status_code == 200
    data = response.json()
    assert "task_id" in data
    assert data["status"] == "queued"
    mock_sqs.send_message.assert_called_once()
    mock_save.assert_called_once()


def test_enqueue_invalid_priority():
    """Priority 'urgent' is not in QUEUES — producer returns 400."""
    response = client.post("/tasks", json={
        "task_type": "send_email",
        "payload": {"to": "user@example.com"},
        "priority": "urgent",
    })
    assert response.status_code in (400, 422)


def test_enqueue_missing_fields():
    """Empty body is missing required fields — Pydantic returns 422."""
    response = client.post("/tasks", json={})
    assert response.status_code == 422


# ---------------------------------------------------------------------------
# Queue depth
# ---------------------------------------------------------------------------

def test_queue_depth_endpoint():
    mock_sqs = MagicMock()
    mock_sqs.get_queue_attributes.return_value = {
        "Attributes": {"ApproximateNumberOfMessages": "7"}
    }

    with patch("producer.main.get_sqs_client", return_value=mock_sqs):
        response = client.get("/queues/depth")

    assert response.status_code == 200
    data = response.json()
    assert "queue_depths" in data
    # One call per queue (high, normal, low, dlq)
    assert mock_sqs.get_queue_attributes.call_count == len(
        [q for q in ["high", "normal", "low", "dlq"]]
    )
