"""
Service layer: reads schedule_league_v2 raw payload from S3 and persists games.
"""
import json
import logging
import os
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional

import boto3

from src.model.models import NbaGame
from src.repository.games_repository import GamesRepository

logger = logging.getLogger(__name__)

_S3_PREFIX = 'raw/schedule_league_v2/'
_DEFAULT_REFRESH_DAYS = 14
_FINAL_GAME_STATUS = 3


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


def _fetch_latest_schedule_document(bucket: str) -> Dict[str, Any]:
    """Find and download the latest schedule_league_v2 document from S3."""
    s3 = _get_s3_client()
    paginator = s3.get_paginator('list_objects_v2')
    pages = paginator.paginate(Bucket=bucket, Prefix=_S3_PREFIX)

    contents: List[Dict[str, Any]] = []
    for page in pages:
        contents.extend(page.get('Contents', []))

    if not contents:
        raise FileNotFoundError(
            f"No schedule_league_v2 raw payloads found in s3://{bucket}/{_S3_PREFIX}"
        )

    latest = max(contents, key=lambda obj: (obj['LastModified'], obj['Key']))
    object_key = latest['Key']
    logger.info("Fetching latest schedule_league_v2 document from s3://%s/%s", bucket, object_key)

    obj = s3.get_object(Bucket=bucket, Key=object_key)
    raw_body = obj['Body'].read().decode('utf-8')
    try:
        return json.loads(raw_body)
    except json.JSONDecodeError as exc:
        raise ValueError(f"Persisted object is not valid JSON: {object_key}") from exc


def _parse_est_datetime(value: str) -> datetime:
    return datetime.fromisoformat(value.replace('Z', '+00:00')).astimezone(timezone.utc)


def _parse_utc_datetime(value: str) -> datetime:
    try:
        return datetime.fromisoformat(value.replace('Z', '+00:00')).astimezone(timezone.utc)
    except ValueError as exc:
        raise ValueError("from_date_utc must be a valid ISO datetime") from exc


def _is_nba_team(team_id: int) -> bool:
    return 1610612700 <= team_id <= 1610612799


def _is_regular_season_game(raw_game: Dict[str, Any]) -> bool:
    game_label = str(raw_game.get('gameLabel', '')).strip().lower()
    if game_label != 'regular season':
        return False

    home_team_id = int(raw_game['homeTeam']['teamId'])
    away_team_id = int(raw_game['awayTeam']['teamId'])
    return _is_nba_team(home_team_id) and _is_nba_team(away_team_id)


def flatten_games(raw_document: Dict[str, Any]) -> List[Dict[str, Any]]:
    """Flatten payload.leagueSchedule.gameDates[].games[] into a single list."""
    payload = raw_document.get('payload', {})
    league_schedule = payload.get('leagueSchedule', {})
    game_dates = league_schedule.get('gameDates')

    if game_dates is None:
        raise ValueError("Persisted object has no payload.leagueSchedule.gameDates field")

    flattened: List[Dict[str, Any]] = []
    for game_date in game_dates:
        flattened.extend(game_date.get('games', []))

    return flattened


def map_games(raw_games: List[Dict[str, Any]]) -> List[NbaGame]:
    games: List[NbaGame] = []
    for raw in raw_games:
        try:
            if _is_regular_season_game(raw):
                games.append(NbaGame.from_raw(raw))
        except (KeyError, ValueError, TypeError) as exc:
            logger.warning("Skipping malformed game record. error=%s", exc)
    return games


def select_candidate_games(
    games: List[NbaGame],
    now_utc: datetime,
    refresh_days: int,
    include_final_games: Optional[bool] = None,
    write_all_season_games: bool = False,
    from_date_utc: Optional[str] = None,
    to_date_utc: Optional[str] = None,
    replay_until_default_horizon: bool = False,
) -> List[NbaGame]:
    """
    Select candidate games using one of three modes:
    - write_all_season_games=True: whole mapped season
    - from_date_utc provided: from that date onward
    - default: today through today + refresh_days
    """
    include_final = include_final_games
    if include_final is None:
        include_final = write_all_season_games

    def is_allowed_by_final_status(game: NbaGame) -> bool:
        return include_final or game.gameStatus != _FINAL_GAME_STATUS

    if write_all_season_games:
        return [game for game in games if is_allowed_by_final_status(game)]

    today_start = now_utc.astimezone(timezone.utc).replace(hour=0, minute=0, second=0, microsecond=0)
    horizon_end = today_start + timedelta(days=refresh_days)

    if from_date_utc:
        start_from = _parse_utc_datetime(from_date_utc)
        end_at: Optional[datetime] = None
        if to_date_utc:
            end_at = _parse_utc_datetime(to_date_utc)
        elif replay_until_default_horizon:
            end_at = horizon_end

        candidates: List[NbaGame] = []
        for game in games:
            game_datetime = _parse_est_datetime(game.gameDateTimeEst)
            if game_datetime < start_from:
                continue
            if end_at is not None and game_datetime > end_at:
                continue
            if is_allowed_by_final_status(game):
                candidates.append(game)
        return candidates

    candidates: List[NbaGame] = []
    for game in games:
        game_datetime = _parse_est_datetime(game.gameDateTimeEst)
        if today_start <= game_datetime <= horizon_end and is_allowed_by_final_status(game):
            candidates.append(game)
    return candidates


class GamesService:
    """Orchestrates reading schedule data from S3 and persisting to DynamoDB."""

    def __init__(self, repository: GamesRepository):
        self.repository = repository

    def fetch_latest_schedule_document(self) -> Dict[str, Any]:
        bucket = _get_bucket_name()
        return _fetch_latest_schedule_document(bucket)

    def consume_games_from_document(
        self,
        raw_document: Dict[str, Any],
        input_options: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, int]:
        raw_games = flatten_games(raw_document)
        mapped_games = map_games(raw_games)

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

        candidates = select_candidate_games(
            mapped_games,
            now_utc=datetime.now(timezone.utc),
            refresh_days=refresh_days,
            include_final_games=include_final_games,
            write_all_season_games=write_all_season_games,
            from_date_utc=from_date_utc,
            to_date_utc=to_date_utc,
            replay_until_default_horizon=replay_until_default_horizon,
        )
        writes = self.repository.upsert_changed_batch(candidates)

        logger.info(
            "Games consumption complete. flattened=%d mapped=%d candidates=%d writes=%d",
            len(raw_games),
            len(mapped_games),
            len(candidates),
            writes,
        )
        return {
            'flattened_games': len(raw_games),
            'mapped_games': len(mapped_games),
            'candidate_games': len(candidates),
            'written_games': writes,
        }

    def consume_games(self, input_options: Optional[Dict[str, Any]] = None) -> Dict[str, int]:
        raw_document = self.fetch_latest_schedule_document()
        return self.consume_games_from_document(raw_document, input_options=input_options)
