"""
Service layer: reads injury_report raw payload from S3 and persists injuries.
"""
import json
import logging
import os
from typing import Any, Dict, List

import boto3

from src.model.models import NbaPlayerInjury
from src.repository.players_injuries_repository import PlayersInjuriesRepository

logger = logging.getLogger(__name__)

_S3_PREFIX = 'raw/injury_report/'


def _get_s3_client():
    return boto3.client('s3')


def _get_bucket_name() -> str:
    bucket = os.environ.get('S3_BUCKET_NAME')
    if not bucket:
        raise ValueError("S3_BUCKET_NAME environment variable is not set")
    return bucket


def _fetch_latest_injury_report_document(bucket: str) -> Dict[str, Any]:
    """Find and download the latest injury_report document from S3."""
    s3 = _get_s3_client()
    paginator = s3.get_paginator('list_objects_v2')
    pages = paginator.paginate(Bucket=bucket, Prefix=_S3_PREFIX)

    contents: List[Dict[str, Any]] = []
    for page in pages:
        contents.extend(page.get('Contents', []))

    if not contents:
        raise FileNotFoundError(
            f"No injury_report raw payloads found in s3://{bucket}/{_S3_PREFIX}"
        )

    latest = max(contents, key=lambda obj: (obj['LastModified'], obj['Key']))
    object_key = latest['Key']
    logger.info("Fetching latest injury_report document from s3://%s/%s", bucket, object_key)

    obj = s3.get_object(Bucket=bucket, Key=object_key)
    raw_body = obj['Body'].read().decode('utf-8')
    try:
        return json.loads(raw_body)
    except json.JSONDecodeError as exc:
        raise ValueError(f"Persisted object is not valid JSON: {object_key}") from exc


def extract_injuries_from_payload(raw_document: Dict[str, Any]) -> List[Dict[str, Any]]:
    """Extract the list of injury entries from payload.injuries."""
    payload = raw_document.get('payload')
    if payload is None:
        raise ValueError("Persisted object has no payload field")
    injuries = payload.get('injuries')
    if injuries is None:
        raise ValueError("Persisted object has no payload.injuries field")
    return list(injuries)


def map_injuries(
    raw_injuries: List[Dict[str, Any]],
    fetched_at: str,
    updated_at: str = None,
) -> List[NbaPlayerInjury]:
    """Convert raw injury entries to NbaPlayerInjury instances.

    Skips entries with null/missing player_id (logs a warning) and any other
    malformed records.
    """
    injuries: List[NbaPlayerInjury] = []
    for raw in raw_injuries:
        if raw.get('player_id') is None:
            logger.warning(
                "Skipping injury entry with null player_id. player_name=%s team_abbr=%s",
                raw.get('player_name'),
                raw.get('team_abbr'),
            )
            continue
        try:
            injuries.append(NbaPlayerInjury.from_raw(raw, fetched_at=fetched_at, updated_at=updated_at))
        except (KeyError, ValueError, TypeError) as exc:
            logger.warning("Skipping malformed injury record. error=%s", exc)
    return injuries


class PlayersInjuriesService:
    """Orchestrates reading injury_report data from S3 and persisting to DynamoDB."""

    def __init__(self, repository: PlayersInjuriesRepository):
        self.repository = repository

    def fetch_latest_injury_report_document(self) -> Dict[str, Any]:
        bucket = _get_bucket_name()
        return _fetch_latest_injury_report_document(bucket)

    def consume_injuries_from_document(
        self,
        raw_document: Dict[str, Any],
    ) -> Dict[str, int]:
        """Parse injury_report raw document, map to NbaPlayerInjury, and persist."""
        raw_injuries = extract_injuries_from_payload(raw_document)
        fetched_at = raw_document.get('fetched_at_utc', '')
        updated_at = raw_document.get('payload', {}).get('updated_at')

        mapped_injuries = map_injuries(raw_injuries, fetched_at=fetched_at, updated_at=updated_at)
        written_injuries = self.repository.upsert_changed_batch(mapped_injuries)

        logger.info(
            "consume_injuries completed. total_injuries=%d, written_injuries=%d",
            len(mapped_injuries),
            written_injuries,
        )

        return {
            'flattened_injuries': len(raw_injuries),
            'mapped_injuries': len(mapped_injuries),
            'written_injuries': written_injuries,
        }
