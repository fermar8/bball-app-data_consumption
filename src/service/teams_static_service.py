"""
Service layer: reads teams_static raw payload from S3 and persists to DynamoDB.
"""
import json
import logging
import os
from typing import Any, Dict, List

import boto3

from src.model.models import NbaTeam
from src.repository.teams_static_repository import TeamsStaticRepository

logger = logging.getLogger(__name__)

_S3_PREFIX = 'raw/teams_static/'


def _get_s3_client():
    return boto3.client('s3')


def _get_bucket_name() -> str:
    bucket = os.environ.get('S3_BUCKET_NAME')
    if not bucket:
        raise ValueError("S3_BUCKET_NAME environment variable is not set")
    return bucket


def _fetch_latest_teams_document(bucket: str) -> Dict[str, Any]:
    """
    Find and download the latest teams_static raw document from S3.

    The S3 key format written by the nba_api_client is:
        raw/teams_static/{year}/{month}/{day}/{hour}/{timestamp}_{hash}.json

    Returns:
        Parsed raw teams_static document.

    Raises:
        FileNotFoundError: When no teams_static objects are found in S3.
        ValueError: When the downloaded object is not valid JSON.
    """
    s3 = _get_s3_client()
    paginator = s3.get_paginator('list_objects_v2')
    pages = paginator.paginate(Bucket=bucket, Prefix=_S3_PREFIX)

    contents: List[Dict[str, Any]] = []
    for page in pages:
        contents.extend(page.get('Contents', []))

    if not contents:
        raise FileNotFoundError(
            f"No teams_static raw payloads found in s3://{bucket}/{_S3_PREFIX}"
        )

    latest = max(contents, key=lambda obj: (obj['LastModified'], obj['Key']))
    object_key = latest['Key']
    logger.info("Fetching latest teams_static document from s3://%s/%s", bucket, object_key)

    obj = s3.get_object(Bucket=bucket, Key=object_key)
    raw_body = obj['Body'].read().decode('utf-8')
    try:
        return json.loads(raw_body)
    except json.JSONDecodeError as exc:
        raise ValueError(f"Persisted object is not valid JSON: {object_key}") from exc


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


class TeamsStaticService:
    """Orchestrates reading teams_static from S3 and persisting to DynamoDB."""

    def __init__(self, repository: TeamsStaticRepository):
        """Initialize the service with a team repository."""
        self.repository = repository

    def fetch_latest_teams_document(self) -> Dict[str, Any]:
        """Fetch the latest teams_static raw document from S3."""
        bucket = _get_bucket_name()
        return _fetch_latest_teams_document(bucket)

    def consume_teams_from_document(self, raw_document: Dict[str, Any]) -> int:
        """
        Map teams from a raw teams_static document and persist to DynamoDB.

        Args:
            raw_document: Parsed JSON document with a payload array.

        Returns:
            Number of teams persisted.

        Raises:
            ValueError: When the document has no payload field.
        """
        payload = raw_document.get('payload')
        if payload is None:
            raise ValueError("Persisted object has no payload field")

        raw_teams: List[dict] = payload
        logger.info("Fetched %d raw team records from S3", len(raw_teams))

        teams = map_teams(raw_teams)
        logger.info("Mapped %d teams, persisting to DynamoDB", len(teams))

        count = self.repository.upsert_batch(teams)
        logger.info("Persisted %d teams to DynamoDB", count)
        return count

    def consume_teams(self) -> int:
        """
        Read the latest teams_static payload from S3, map it, and persist to DynamoDB.

        Returns:
            Number of teams persisted.

        Raises:
            FileNotFoundError: When no S3 data is available.
            ValueError: When the S3 object is malformed or env vars are missing.
        """
        raw_document = self.fetch_latest_teams_document()
        return self.consume_teams_from_document(raw_document)
