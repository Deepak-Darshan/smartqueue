"""
SmartQueue AI Ops Assistant — powered by Claude.

Exposes run_assistant(question) for use by the FastAPI producer endpoints
and a standalone interactive CLI when executed directly.
"""

import json
import os
import sys

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import anthropic  # noqa: E402
from dotenv import load_dotenv  # noqa: E402

from shared.config import get_sqs_client, QUEUES  # noqa: E402
from shared.db import get_task, list_tasks  # noqa: E402

load_dotenv()

MODEL = "claude-sonnet-4-20250514"

SYSTEM_PROMPT = (
    "You are an intelligent ops assistant for SmartQueue, a distributed task queue system "
    "running on AWS. You have access to real-time queue metrics and task data. When asked "
    "about system health, diagnose issues clearly and suggest specific actionable fixes. "
    "Be concise and technical."
)

# ---------------------------------------------------------------------------
# Tool definitions (sent to Claude)
# ---------------------------------------------------------------------------

TOOLS = [
    {
        "name": "get_queue_depths",
        "description": (
            "Get the current approximate message count for all SQS queues "
            "(high, normal, low, dlq). Use this to check queue backlog or "
            "detect DLQ spikes."
        ),
        "input_schema": {
            "type": "object",
            "properties": {},
            "required": [],
        },
    },
    {
        "name": "get_failed_tasks",
        "description": (
            "Retrieve the last 10 tasks with status 'failed' from DynamoDB. "
            "Returns task_id, task_type, error message, and timestamp. Use this "
            "to identify what is failing and why."
        ),
        "input_schema": {
            "type": "object",
            "properties": {},
            "required": [],
        },
    },
    {
        "name": "get_task_details",
        "description": (
            "Get full details for a specific task by its task_id, including "
            "payload, status, attempt count, error, and timestamps."
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "task_id": {
                    "type": "string",
                    "description": "The unique UUID of the task to look up.",
                }
            },
            "required": ["task_id"],
        },
    },
    {
        "name": "get_queue_stats",
        "description": (
            "Scan the last 100 tasks in DynamoDB and return aggregate statistics: "
            "total_tasks, success_rate (%), failure_rate (%), avg_attempts, and "
            "a breakdown of tasks by status. Use this for a system health overview."
        ),
        "input_schema": {
            "type": "object",
            "properties": {},
            "required": [],
        },
    },
]


# ---------------------------------------------------------------------------
# Tool implementations (called by the assistant loop)
# ---------------------------------------------------------------------------

def _get_queue_depths(_input: dict) -> str:
    sqs = get_sqs_client()
    depths = {}
    for name, url in QUEUES.items():
        resp = sqs.get_queue_attributes(
            QueueUrl=url,
            AttributeNames=["ApproximateNumberOfMessages"],
        )
        depths[name] = int(resp["Attributes"]["ApproximateNumberOfMessages"])
    return json.dumps({"queue_depths": depths})


def _get_failed_tasks(_input: dict) -> str:
    tasks = list_tasks(status="failed", limit=10)
    results = []
    for t in tasks:
        results.append({
            "task_id": t.get("task_id"),
            "task_type": t.get("task_type"),
            "error": t.get("error"),
            "attempts": t.get("attempts"),
            "created_at": t.get("created_at"),
            "updated_at": t.get("updated_at"),
        })
    return json.dumps({"failed_tasks": results, "count": len(results)})


def _get_task_details(input_data: dict) -> str:
    task_id = input_data.get("task_id", "")
    task = get_task(task_id)
    if task is None:
        return json.dumps({"error": f"Task '{task_id}' not found."})
    return json.dumps(task)


def _get_queue_stats(_input: dict) -> str:
    tasks = list_tasks(limit=100)
    total = len(tasks)
    if total == 0:
        return json.dumps({
            "total_tasks": 0,
            "success_rate": 0.0,
            "failure_rate": 0.0,
            "avg_attempts": 0.0,
            "tasks_by_status": {},
        })

    by_status: dict = {}
    total_attempts = 0
    for task in tasks:
        status = task.get("status", "unknown")
        by_status[status] = by_status.get(status, 0) + 1
        total_attempts += task.get("attempts", 0)

    completed = by_status.get("completed", 0)
    failed = by_status.get("failed", 0) + by_status.get("dead_lettered", 0)

    return json.dumps({
        "total_tasks": total,
        "success_rate": round(completed / total * 100, 1),
        "failure_rate": round(failed / total * 100, 1),
        "avg_attempts": round(total_attempts / total, 2),
        "tasks_by_status": by_status,
    })


# Map tool names → Python functions
_TOOL_MAP = {
    "get_queue_depths": _get_queue_depths,
    "get_failed_tasks": _get_failed_tasks,
    "get_task_details": _get_task_details,
    "get_queue_stats": _get_queue_stats,
}


# ---------------------------------------------------------------------------
# Core assistant logic
# ---------------------------------------------------------------------------

def run_assistant(user_question: str) -> str:
    """
    Send user_question to Claude with all 4 ops tools.
    Runs the agentic loop until Claude returns a final text response.
    Returns Claude's final answer as a plain string.
    """
    client = anthropic.Anthropic()
    messages = [{"role": "user", "content": user_question}]

    while True:
        response = client.messages.create(
            model=MODEL,
            max_tokens=4096,
            system=SYSTEM_PROMPT,
            tools=TOOLS,
            messages=messages,
        )

        # Claude finished — extract the final text
        if response.stop_reason == "end_turn":
            text_block = next(
                (b for b in response.content if b.type == "text"), None
            )
            return text_block.text if text_block else "(no response)"

        # Claude wants to call one or more tools
        if response.stop_reason == "tool_use":
            # Append assistant turn (must include tool_use blocks)
            messages.append({"role": "assistant", "content": response.content})

            tool_results = []
            for block in response.content:
                if block.type != "tool_use":
                    continue

                fn = _TOOL_MAP.get(block.name)
                if fn is None:
                    content = f"Unknown tool: {block.name}"
                    is_error = True
                else:
                    try:
                        content = fn(block.input)
                        is_error = False
                    except Exception as exc:
                        content = f"Tool '{block.name}' raised an error: {exc}"
                        is_error = True

                tool_results.append({
                    "type": "tool_result",
                    "tool_use_id": block.id,
                    "content": content,
                    "is_error": is_error,
                })

            messages.append({"role": "user", "content": tool_results})
            continue

        # Unexpected stop reason (e.g. max_tokens) — return whatever text exists
        text_block = next(
            (b for b in response.content if b.type == "text"), None
        )
        return text_block.text if text_block else f"Stopped: {response.stop_reason}"


# ---------------------------------------------------------------------------
# Interactive CLI
# ---------------------------------------------------------------------------

def _cli() -> None:
    print("=" * 60)
    print("  SmartQueue AI Assistant — powered by Claude")
    print("  Type 'exit' or 'quit' to stop.")
    print("=" * 60)
    print()

    while True:
        try:
            question = input("You: ").strip()
        except (KeyboardInterrupt, EOFError):
            print("\nGoodbye.")
            break

        if not question:
            continue

        if question.lower() in ("exit", "quit"):
            print("Goodbye.")
            break

        try:
            answer = run_assistant(question)
            print(f"\nAssistant: {answer}\n")
        except anthropic.APIError as exc:
            print(f"\n[API error] {exc}\n")
        except Exception as exc:
            print(f"\n[Error] {exc}\n")


if __name__ == "__main__":
    _cli()
