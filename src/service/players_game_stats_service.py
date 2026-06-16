"""
Service layer: reads player_game_logs raw payload from S3 and persists player game logs.
"""
import json
import logging
import os
from typing import Any, Dict, List

import boto3

from src.model.models import NbaPlayerGameLog
from src.repository.players_game_stats_repository import PlayersGameStatsRepository

logger = logging.getLogger(__name__)

_S3_PREFIX = 'raw/player_game_logs/'


def _get_s3_client():
    return boto3.client('s3')


def _get_bucket_name() -> str:
    bucket = os.environ.get('S3_BUCKET_NAME')
    if not bucket:
        raise ValueError("S3_BUCKET_NAME environment variable is not set")
    return bucket


def _fetch_latest_player_game_logs_document(bucket: str) -> Dict[str, Any]:
    """Find and download the latest player_game_logs document from S3."""
    s3 = _get_s3_client()
    paginator = s3.get_paginator('list_objects_v2')
    pages = paginator.paginate(Bucket=bucket, Prefix=_S3_PREFIX)

    contents: List[Dict[str, Any]] = []
    for page in pages:
        contents.extend(page.get('Contents', []))

    if not contents:
        raise FileNotFoundError(
            f"No player_game_logs raw payloads found in s3://{bucket}/{_S3_PREFIX}"
        )

    latest = max(contents, key=lambda obj: (obj['LastModified'], obj['Key']))
    object_key = latest['Key']
    logger.info("Fetching latest player_game_logs document from s3://%s/%s", bucket, object_key)

    obj = s3.get_object(Bucket=bucket, Key=object_key)
    raw_body = obj['Body'].read().decode('utf-8')
    try:
        return json.loads(raw_body)
    except json.JSONDecodeError as exc:
        raise ValueError(f"Persisted object is not valid JSON: {object_key}") from exc


def extract_player_game_logs_from_payload(raw_document: Dict[str, Any]) -> List[Dict[str, Any]]:
    """
    Extract player game log rows from payload.resultSets[?name=='PlayerGameLogs'].rowSet.
    Returns list of dictionaries with headers mapped to values.
    """
    payload = raw_document.get('payload', {})
    result_sets = payload.get('resultSets', [])

    if not result_sets:
        raise ValueError("Persisted object has no payload.resultSets field")

    player_game_logs_set = None
    for result_set in result_sets:
        if result_set.get('name') == 'PlayerGameLogs':
            player_game_logs_set = result_set
            break

    if player_game_logs_set is None:
        raise ValueError("No PlayerGameLogs result set found in payload.resultSets")

    headers = player_game_logs_set.get('headers', [])
    row_set = player_game_logs_set.get('rowSet', [])

    if not headers:
        raise ValueError("PlayerGameLogs result set has no headers")

    player_game_logs: List[Dict[str, Any]] = []
    for row in row_set:
        player_game_log_dict = {}
        for index, header in enumerate(headers):
            if index < len(row):
                player_game_log_dict[header] = row[index]
        player_game_logs.append(player_game_log_dict)

    return player_game_logs


def map_player_game_logs(raw_player_game_logs: List[Dict[str, Any]]) -> List[NbaPlayerGameLog]:
    """
    Convert raw player game log rows to NbaPlayerGameLog instances.
    Skips malformed records and logs warnings.
    """
    player_game_logs: List[NbaPlayerGameLog] = []
    for raw in raw_player_game_logs:
        try:
            player_game_logs.append(NbaPlayerGameLog.from_raw(raw))
        except (KeyError, ValueError, TypeError) as exc:
            logger.warning("Skipping malformed player game log record. error=%s", exc)
    return player_game_logs


class PlayersGameStatsService:
    """Orchestrates reading player game logs from S3 and persisting to DynamoDB."""

    def __init__(self, repository: PlayersGameStatsRepository):
        self.repository = repository

    def fetch_latest_player_game_logs_document(self) -> Dict[str, Any]:
        bucket = _get_bucket_name()
        return _fetch_latest_player_game_logs_document(bucket)

    def consume_player_game_logs_from_document(self, raw_document: Dict[str, Any]) -> Dict[str, int]:
        raw_player_game_logs = extract_player_game_logs_from_payload(raw_document)
        mapped_player_game_logs = map_player_game_logs(raw_player_game_logs)

        written_player_game_logs = self.repository.upsert_changed_batch(mapped_player_game_logs)

        logger.info(
            "consume_player_game_logs completed. total_rows=%d, written_rows=%d",
            len(mapped_player_game_logs),
            written_player_game_logs,
        )

        return {
            'flattened_player_game_logs': len(raw_player_game_logs),
            'mapped_player_game_logs': len(mapped_player_game_logs),
            'written_player_game_logs': written_player_game_logs,
        }

    def consume_player_game_logs(self) -> Dict[str, int]:
        raw_document = self.fetch_latest_player_game_logs_document()
        return self.consume_player_game_logs_from_document(raw_document)
