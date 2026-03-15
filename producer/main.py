from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from typing import Optional
import json
import uuid
import time
import sys
import os

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from shared.config import get_sqs_client, QUEUES

app = FastAPI(title="SmartQueue Producer", version="1.0.0")

class Task(BaseModel):
    task_type: str
    payload: dict
    priority: Optional[str] = "normal"
    max_retries: Optional[int] = 3

@app.get("/health")
def health():
    return {"status": "ok", "service": "producer"}

@app.post("/tasks")
def enqueue_task(task: Task):
    if task.priority not in QUEUES:
        raise HTTPException(status_code=400, detail=f"Invalid priority. Use: high, normal, low")

    task_id = str(uuid.uuid4())
    message = {
        "task_id": task_id,
        "task_type": task.task_type,
        "payload": task.payload,
        "priority": task.priority,
        "max_retries": task.max_retries,
        "created_at": time.time(),
        "attempts": 0
    }

    sqs = get_sqs_client()
    sqs.send_message(
        QueueUrl=QUEUES[task.priority],
        MessageBody=json.dumps(message),
        MessageAttributes={
            "task_type": {
                "StringValue": task.task_type,
                "DataType": "String"
            }
        }
    )

    return {
        "task_id": task_id,
        "status": "queued",
        "priority": task.priority,
        "queue": QUEUES[task.priority]
    }

@app.get("/queues/depth")
def queue_depth():
    sqs = get_sqs_client()
    depths = {}
    for name, url in QUEUES.items():
        response = sqs.get_queue_attributes(
            QueueUrl=url,
            AttributeNames=["ApproximateNumberOfMessages"]
        )
        depths[name] = int(response["Attributes"]["ApproximateNumberOfMessages"])
    return {"queue_depths": depths}
