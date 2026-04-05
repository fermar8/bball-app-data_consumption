"""
Service layer: reads teams_static raw payload from S3 and persists to DynamoDB.
"""
import json
import logging
import os
from typing import List

import boto3

from src.model.models import NbaTeam
from src.repository.repository import TeamRepository

logger = logging.getLogger(__name__)

_S3_PREFIX = 'raw/teams_static/'


def _get_s3_client():
    return boto3.client('s3')


def _get_bucket_name() -> str:
    bucket = os.environ.get('S3_BUCKET_NAME')
    if not bucket:
        raise ValueError("S3_BUCKET_NAME environment variable is not set")
    return bucket


def _fetch_latest_teams_payload(bucket: str) -> List[dict]:
    """
    Find and download the latest teams_static raw payload from S3.

    The S3 key format written by the nba_api_client is:
        raw/teams_static/{year}/{month}/{day}/{hour}/{timestamp}_{hash}.json

    Returns:
        List of raw team dicts from the payload field.

    Raises:
        FileNotFoundError: When no teams_static objects are found in S3.
        ValueError: When the downloaded object has no payload field.
    """
    s3 = _get_s3_client()
    listing = s3.list_objects_v2(Bucket=bucket, Prefix=_S3_PREFIX)
    contents = listing.get('Contents', [])

    if not contents:
        raise FileNotFoundError(
            f"No teams_static raw payloads found in s3://{bucket}/{_S3_PREFIX}"
        )

    latest = max(contents, key=lambda obj: obj['LastModified'])
    object_key = latest['Key']
    logger.info("Fetching teams_static payload from s3://%s/%s", bucket, object_key)

    obj = s3.get_object(Bucket=bucket, Key=object_key)
    raw_body = obj['Body'].read().decode('utf-8')
    parsed = json.loads(raw_body)

    payload = parsed.get('payload')
    if payload is None:
        raise ValueError(f"Persisted object has no payload field: {object_key}")

    return payload


def map_teams(raw_teams: List[dict]) -> List[NbaTeam]:
    """
    Map a list of raw nba_api team dicts to NbaTeam model instances.

    Args:
        raw_teams: List of raw team dicts with keys id, full_name, etc.

    Returns:
        List of NbaTeam instances.
    """
    teams: List[NbaTeam] = []
    for raw in raw_teams:
        try:
            teams.append(NbaTeam.from_raw(raw))
        except (KeyError, ValueError, TypeError) as exc:
            logger.warning("Skipping malformed team record: %s error=%s", raw, exc)
    return teams


class TeamsConsumptionService:
    """Orchestrates reading teams_static from S3 and persisting to DynamoDB."""

    def __init__(self, repository: TeamRepository):
        """Initialize the service with a team repository."""
        self.repository = repository

    def consume_teams(self) -> int:
        """
        Read the latest teams_static payload from S3, map it, and persist to DynamoDB.

        Returns:
            Number of teams persisted.

        Raises:
            FileNotFoundError: When no S3 data is available.
            ValueError: When the S3 object is malformed or env vars are missing.
        """
        bucket = _get_bucket_name()
        raw_teams = _fetch_latest_teams_payload(bucket)
        logger.info("Fetched %d raw team records from S3", len(raw_teams))

        teams = map_teams(raw_teams)
        logger.info("Mapped %d teams, persisting to DynamoDB", len(teams))

        count = self.repository.upsert_batch(teams)
        logger.info("Persisted %d teams to DynamoDB", count)
        return count
