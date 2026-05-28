"""
Service layer: reads player_index raw payload from S3 and persists players.
"""
import json
import logging
import os
from typing import Any, Dict, List

import boto3

from src.model.models import NbaPlayer
from src.repository.players_index_repository import PlayersIndexRepository

logger = logging.getLogger(__name__)

_S3_PREFIX = 'raw/player_index/'


def _get_s3_client():
    return boto3.client('s3')


def _get_bucket_name() -> str:
    bucket = os.environ.get('S3_BUCKET_NAME')
    if not bucket:
        raise ValueError("S3_BUCKET_NAME environment variable is not set")
    return bucket


def _fetch_latest_player_index_document(bucket: str) -> Dict[str, Any]:
    """Find and download the latest player_index document from S3."""
    s3 = _get_s3_client()
    paginator = s3.get_paginator('list_objects_v2')
    pages = paginator.paginate(Bucket=bucket, Prefix=_S3_PREFIX)

    contents: List[Dict[str, Any]] = []
    for page in pages:
        contents.extend(page.get('Contents', []))

    if not contents:
        raise FileNotFoundError(
            f"No player_index raw payloads found in s3://{bucket}/{_S3_PREFIX}"
        )

    latest = max(contents, key=lambda obj: (obj['LastModified'], obj['Key']))
    object_key = latest['Key']
    logger.info("Fetching latest player_index document from s3://%s/%s", bucket, object_key)

    obj = s3.get_object(Bucket=bucket, Key=object_key)
    raw_body = obj['Body'].read().decode('utf-8')
    try:
        return json.loads(raw_body)
    except json.JSONDecodeError as exc:
        raise ValueError(f"Persisted object is not valid JSON: {object_key}") from exc


def extract_players_from_payload(raw_document: Dict[str, Any]) -> List[Dict[str, Any]]:
    """
    Extract player rows from payload.resultSets[?name=='PlayerIndex'].rowSet.
    Returns list of player dictionaries with headers mapped to values.
    """
    payload = raw_document.get('payload', {})
    result_sets = payload.get('resultSets', [])

    if not result_sets:
        raise ValueError("Persisted object has no payload.resultSets field")

    # Find the PlayerIndex result set
    player_index_set = None
    for result_set in result_sets:
        if result_set.get('name') == 'PlayerIndex':
            player_index_set = result_set
            break

    if player_index_set is None:
        raise ValueError("No PlayerIndex result set found in payload.resultSets")

    headers = player_index_set.get('headers', [])
    row_set = player_index_set.get('rowSet', [])

    if not headers:
        raise ValueError("PlayerIndex result set has no headers")

    # Map each row to a dictionary using headers
    players: List[Dict[str, Any]] = []
    for row in row_set:
        player_dict = {}
        for i, header in enumerate(headers):
            if i < len(row):
                player_dict[header] = row[i]
        players.append(player_dict)

    return players


def map_players(raw_players: List[Dict[str, Any]]) -> List[NbaPlayer]:
    """
    Convert raw player rows to NbaPlayer instances.
    Skips malformed records and logs warnings.
    """
    players: List[NbaPlayer] = []
    for raw in raw_players:
        try:
            players.append(NbaPlayer.from_raw(raw))
        except (KeyError, ValueError, TypeError) as exc:
            logger.warning("Skipping malformed player record. error=%s", exc)
    return players


class PlayersIndexService:
    """Orchestrates reading player index data from S3 and persisting to DynamoDB."""

    def __init__(self, repository: PlayersIndexRepository):
        self.repository = repository

    def fetch_latest_player_index_document(self) -> Dict[str, Any]:
        bucket = _get_bucket_name()
        return _fetch_latest_player_index_document(bucket)

    def consume_players_from_document(
        self,
        raw_document: Dict[str, Any],
    ) -> Dict[str, int]:
        """
        Parse player_index raw document, map to NbaPlayer models, and persist.
        Returns stats about the operation.
        """
        raw_players = extract_players_from_payload(raw_document)
        mapped_players = map_players(raw_players)

        written_players = self.repository.upsert_changed_batch(mapped_players)

        logger.info(
            "consume_players completed. total_players=%d, written_players=%d",
            len(mapped_players),
            written_players,
        )

        return {
            'flattened_players': len(raw_players),
            'mapped_players': len(mapped_players),
            'written_players': written_players,
        }
