"""
Service layer: reads player_game_logs raw payload from S3 and persists player game logs.
"""
import json
import logging
import os
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional

import boto3

from src.model.models import NbaPlayerGameLog
from src.repository.players_game_stats_repository import PlayersGameStatsRepository

logger = logging.getLogger(__name__)

_S3_PREFIX = 'raw/player_game_logs/'
_DEFAULT_REFRESH_DAYS = 14


def _get_s3_client():
    return boto3.client('s3')


def _get_bucket_name() -> str:
    bucket = os.environ.get('S3_BUCKET_NAME')
    if not bucket:
        raise ValueError("S3_BUCKET_NAME environment variable is not set")
    return bucket


def _get_refresh_days() -> int:
    raw = os.environ.get('GAMES_REFRESH_DAYS', str(_DEFAULT_REFRESH_DAYS))
    try:
        value = int(raw)
    except ValueError as exc:
        raise ValueError("GAMES_REFRESH_DAYS must be an integer") from exc
    if value < 1:
        raise ValueError("GAMES_REFRESH_DAYS must be >= 1")
    return value


def _parse_utc_datetime(value: str) -> datetime:
    try:
        return datetime.fromisoformat(value.replace('Z', '+00:00')).astimezone(timezone.utc)
    except ValueError as exc:
        raise ValueError("must be a valid ISO datetime") from exc


def _parse_game_date(value: str) -> datetime:
    normalized = str(value).strip()
    if not normalized:
        raise ValueError("missing GAME_DATE")
    if 'T' in normalized:
        return datetime.fromisoformat(normalized.replace('Z', '+00:00')).astimezone(timezone.utc)
    return datetime.strptime(normalized, '%Y-%m-%d').replace(tzinfo=timezone.utc)


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


def select_candidate_player_game_logs(
    player_game_logs: List[NbaPlayerGameLog],
    now_utc: Optional[datetime] = None,
    refresh_days: int = _DEFAULT_REFRESH_DAYS,
    write_all_season_games: bool = False,
    from_date_utc: Optional[str] = None,
    to_date_utc: Optional[str] = None,
    replay_until_default_horizon: bool = False,
) -> List[NbaPlayerGameLog]:
    """Filter player game logs by an ad-hoc manual date range when provided."""
    if write_all_season_games:
        return list(player_game_logs)

    if not from_date_utc:
        return list(player_game_logs)

    start_from = _parse_utc_datetime(from_date_utc)
    end_at: Optional[datetime] = None
    if to_date_utc:
        end_at = _parse_utc_datetime(to_date_utc)
    elif replay_until_default_horizon:
        horizon_now = (now_utc or datetime.now(timezone.utc)).astimezone(timezone.utc)
        end_at = horizon_now.replace(hour=0, minute=0, second=0, microsecond=0) + timedelta(days=refresh_days)

    candidates: List[NbaPlayerGameLog] = []
    for player_game_log in player_game_logs:
        try:
            game_ts = _parse_game_date(player_game_log.gameDate)
        except ValueError:
            continue
        if game_ts < start_from:
            continue
        if end_at is not None and game_ts > end_at:
            continue
        candidates.append(player_game_log)
    return candidates


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

    def consume_player_game_logs_from_document(
        self,
        raw_document: Dict[str, Any],
        input_options: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, int]:
        raw_player_game_logs = extract_player_game_logs_from_payload(raw_document)
        mapped_player_game_logs = map_player_game_logs(raw_player_game_logs)

        options = input_options or {}

        refresh_days_input = options.get('refresh_days')
        if refresh_days_input is not None:
            try:
                refresh_days = int(refresh_days_input)
            except ValueError as exc:
                raise ValueError("refresh_days must be an integer") from exc
            if refresh_days < 1:
                raise ValueError("refresh_days must be >= 1")
        else:
            refresh_days = _get_refresh_days()

        write_all_season_games = options.get('write_all_season_games', False)
        if not isinstance(write_all_season_games, bool):
            raise ValueError("write_all_season_games must be a boolean")

        include_final_games = options.get('include_final_games')
        if include_final_games is not None and not isinstance(include_final_games, bool):
            raise ValueError("include_final_games must be a boolean")

        from_date_utc = options.get('from_date_utc')
        if from_date_utc is not None and not isinstance(from_date_utc, str):
            raise ValueError("from_date_utc must be an ISO datetime string")

        to_date_utc = options.get('to_date_utc')
        if to_date_utc is not None and not isinstance(to_date_utc, str):
            raise ValueError("to_date_utc must be an ISO datetime string")

        replay_until_default_horizon = options.get('replay_until_default_horizon', False)
        if not isinstance(replay_until_default_horizon, bool):
            raise ValueError("replay_until_default_horizon must be a boolean")

        if to_date_utc and not from_date_utc:
            raise ValueError("to_date_utc requires from_date_utc")

        if to_date_utc and replay_until_default_horizon:
            raise ValueError("to_date_utc and replay_until_default_horizon are mutually exclusive")

        if write_all_season_games and (from_date_utc or to_date_utc or replay_until_default_horizon):
            raise ValueError(
                "write_all_season_games cannot be combined with replay range options"
            )

        if from_date_utc and to_date_utc:
            from_dt = _parse_utc_datetime(from_date_utc)
            to_dt = _parse_utc_datetime(to_date_utc)
            if to_dt < from_dt:
                raise ValueError("to_date_utc must be greater than or equal to from_date_utc")

        if replay_until_default_horizon and not from_date_utc:
            raise ValueError("replay_until_default_horizon requires from_date_utc")

        candidates = select_candidate_player_game_logs(
            mapped_player_game_logs,
            now_utc=datetime.now(timezone.utc),
            refresh_days=refresh_days,
            write_all_season_games=write_all_season_games,
            from_date_utc=from_date_utc,
            to_date_utc=to_date_utc,
            replay_until_default_horizon=replay_until_default_horizon,
        )

        written_player_game_logs = self.repository.upsert_changed_batch(candidates)

        logger.info(
            "consume_player_game_logs completed. total_rows=%d, candidate_rows=%d, written_rows=%d",
            len(mapped_player_game_logs),
            len(candidates),
            written_player_game_logs,
        )

        return {
            'flattened_player_game_logs': len(raw_player_game_logs),
            'mapped_player_game_logs': len(mapped_player_game_logs),
            'candidate_player_game_logs': len(candidates),
            'written_player_game_logs': written_player_game_logs,
        }

    def consume_player_game_logs(
        self,
        input_options: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, int]:
        raw_document = self.fetch_latest_player_game_logs_document()
        return self.consume_player_game_logs_from_document(raw_document, input_options=input_options)
