import json
import boto3


def get_secret(secret_name: str) -> dict:
    """connects to a boto3 session and gets a secret from secrets manager."""
