import boto3
from dotenv import load_dotenv
import os

load_dotenv()

AWS_REGION = os.getenv("AWS_REGION", "ap-southeast-2")

QUEUES = {
    "high":   os.getenv("SQS_HIGH_QUEUE"),
    "normal": os.getenv("SQS_NORMAL_QUEUE"),
    "low":    os.getenv("SQS_LOW_QUEUE"),
    "dlq":    os.getenv("SQS_DLQ"),
}

def get_sqs_client():
    return boto3.client("sqs", region_name=AWS_REGION)


TABLE_NAME = "smartqueue-tasks"

def get_dynamodb_client():
    return boto3.client("dynamodb", region_name=AWS_REGION)


# Path where the scaler writes its state so the producer API can read it
SCALER_STATE_FILE = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), ".scaler_state.json")
