"""
Run this script once to create the DynamoDB table and enable TTL.

    python scripts/create_table.py
"""
import sys
import os

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from shared.config import TABLE_NAME
from shared.db import create_table

if __name__ == "__main__":
    print(f"Creating DynamoDB table '{TABLE_NAME}' ...")
    table = create_table()
    print(f"Done. Status: {table.get('TableStatus', 'ACTIVE')}")
