"""
DynamoDB helpers for SmartQueue task tracking.

Table schema
------------
Partition key : task_id  (S)
TTL attribute : expires_at  (N, Unix epoch seconds)

All other fields are stored as a flat map of DynamoDB AttributeValues.
"""

import time
from typing import Optional

from shared.config import get_dynamodb_client, TABLE_NAME

_TTL_DAYS = 7


# ---------------------------------------------------------------------------
# Table lifecycle
# ---------------------------------------------------------------------------

def create_table() -> dict:
    """Create the DynamoDB table. Safe to call when the table already exists."""
    client = get_dynamodb_client()

    try:
        response = client.create_table(
            TableName=TABLE_NAME,
            KeySchema=[
                {"AttributeName": "task_id", "KeyType": "HASH"},
            ],
            AttributeDefinitions=[
                {"AttributeName": "task_id", "AttributeType": "S"},
            ],
            BillingMode="PAY_PER_REQUEST",
        )
        # Enable TTL
        client.update_time_to_live(
            TableName=TABLE_NAME,
            TimeToLiveSpecification={
                "Enabled": True,
                "AttributeName": "expires_at",
            },
        )
        return response["TableDescription"]
    except client.exceptions.ResourceInUseException:
        # Table already exists — return its description instead
        return client.describe_table(TableName=TABLE_NAME)["Table"]


# ---------------------------------------------------------------------------
# Internal helpers — DynamoDB ↔ Python dict conversion
# ---------------------------------------------------------------------------

def _to_dynamo(value) -> dict:
    """Convert a Python scalar/dict/list to a DynamoDB AttributeValue."""
    if isinstance(value, bool):
        return {"BOOL": value}
    if isinstance(value, (int, float)):
        return {"N": str(value)}
    if isinstance(value, str):
        return {"S": value}
    if isinstance(value, dict):
        return {"M": {k: _to_dynamo(v) for k, v in value.items()}}
    if isinstance(value, list):
        return {"L": [_to_dynamo(v) for v in value]}
    if value is None:
        return {"NULL": True}
    return {"S": str(value)}


def _from_dynamo(attr: dict):
    """Convert a DynamoDB AttributeValue back to a plain Python value."""
    if "S" in attr:
        return attr["S"]
    if "N" in attr:
        v = attr["N"]
        return int(v) if "." not in v else float(v)
    if "BOOL" in attr:
        return attr["BOOL"]
    if "NULL" in attr:
        return None
    if "M" in attr:
        return {k: _from_dynamo(v) for k, v in attr["M"].items()}
    if "L" in attr:
        return [_from_dynamo(v) for v in attr["L"]]
    return attr  # fallback


def _deserialize(item: dict) -> dict:
    return {k: _from_dynamo(v) for k, v in item.items()}


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def save_task(task: dict) -> None:
    """
    Persist a task with status='queued' and a 7-day TTL.
    The task dict should already contain task_id, task_type, payload, priority, etc.
    """
    client = get_dynamodb_client()
    now = time.time()
    item = {**task, "status": "queued", "created_at": now,
            "expires_at": int(now + _TTL_DAYS * 86400)}
    client.put_item(
        TableName=TABLE_NAME,
        Item={k: _to_dynamo(v) for k, v in item.items()},
    )


def update_task_status(
    task_id: str,
    status: str,
    result=None,
    error: Optional[str] = None,
) -> None:
    """
    Update the status field of a task and record a timestamp.

    status values used by this system: 'processing', 'completed', 'failed', 'dead_lettered'
    """
    client = get_dynamodb_client()
    now = time.time()

    # Build update expression dynamically so we only write provided fields
    updates = {"#s": ":status", "updated_at": ":updated_at"}
    expr_names = {"#s": "status"}
    expr_values = {":status": _to_dynamo(status), ":updated_at": _to_dynamo(now)}

    if result is not None:
        expr_names["#r"] = "result"
        updates["#r"] = ":result"
        expr_values[":result"] = _to_dynamo(result)

    if error is not None:
        expr_names["#e"] = "error"
        updates["#e"] = ":error"
        expr_values[":error"] = _to_dynamo(error)

    set_clause = ", ".join(f"{k} = {v}" for k, v in updates.items())

    client.update_item(
        TableName=TABLE_NAME,
        Key={"task_id": {"S": task_id}},
        UpdateExpression=f"SET {set_clause}",
        ExpressionAttributeNames=expr_names,
        ExpressionAttributeValues=expr_values,
    )


def get_task(task_id: str) -> Optional[dict]:
    """Return a task by ID, or None if not found."""
    client = get_dynamodb_client()
    response = client.get_item(
        TableName=TABLE_NAME,
        Key={"task_id": {"S": task_id}},
    )
    item = response.get("Item")
    return _deserialize(item) if item else None


def list_tasks(status: Optional[str] = None, limit: int = 20) -> list[dict]:
    """
    Scan up to `limit` tasks, optionally filtered by status.
    Uses a Scan (acceptable at internship/demo scale; a GSI on status would be
    the production upgrade).
    """
    client = get_dynamodb_client()
    kwargs: dict = {"TableName": TABLE_NAME, "Limit": limit}

    if status:
        kwargs["FilterExpression"] = "#s = :status"
        kwargs["ExpressionAttributeNames"] = {"#s": "status"}
        kwargs["ExpressionAttributeValues"] = {":status": {"S": status}}

    response = client.scan(**kwargs)
    return [_deserialize(item) for item in response.get("Items", [])]
