"""
Unit tests for the games service layer with mocked dependencies.
"""
from datetime import datetime, timezone
from unittest.mock import MagicMock, patch

import pytest

from src.model.models import NbaGame
from src.service.games_service import GamesService
from src.service.games_service import flatten_games, map_games, select_candidate_games


RAW_DOCUMENT = {
    'payload': {
        'leagueSchedule': {
            'gameDates': [
                {
                    'gameDate': '10/22/2025 00:00:00',
                    'games': [
                        {
                            'gameId': '0022500001',
                            'gameStatus': 1,
                            'gameStatusText': 'Scheduled',
                            'gameDateEst': '2099-10-22T00:00:00Z',
                            'gameDateTimeEst': '2099-10-22T19:30:00Z',
                            'gameLabel': 'Regular Season',
                            'homeTeam': {
                                'teamId': 1610612747,
                                'teamName': 'Lakers',
                                'teamTricode': 'LAL',
                                'wins': 0,
                                'losses': 0,
                                'score': None,
                            },
                            'awayTeam': {
                                'teamId': 1610612738,
                                'teamName': 'Celtics',
                                'teamTricode': 'BOS',
                                'wins': 0,
                                'losses': 0,
                                'score': None,
                            },
                            'arenaName': 'Crypto.com Arena',
                            'arenaCity': 'Los Angeles',
                        },
                        {
                            'gameId': '0012500001',
                            'gameStatus': 1,
                            'gameStatusText': 'Scheduled',
                            'gameDateEst': '2025-10-03T00:00:00Z',
                            'gameDateTimeEst': '2025-10-03T20:30:00Z',
                            'gameLabel': 'Preseason',
                            'homeTeam': {
                                'teamId': 1610612747,
                                'teamName': 'Lakers',
                                'teamTricode': 'LAL',
                            },
                            'awayTeam': {
                                'teamId': 1610612756,
                                'teamName': 'Suns',
                                'teamTricode': 'PHX',
                            },
                        },
                    ],
                }
            ]
        }
    }
}


class TestFlattenGames:
    def test_flattens_games_from_all_dates(self):
        flattened = flatten_games(RAW_DOCUMENT)
        assert len(flattened) == 2

    def test_raises_when_missing_game_dates(self):
        with pytest.raises(ValueError, match='gameDates'):
            flatten_games({'payload': {'leagueSchedule': {}}})


class TestMapGames:
    def test_keeps_only_regular_season_nba_games(self):
        games = map_games(flatten_games(RAW_DOCUMENT))
        assert len(games) == 1
        assert games[0].gameId == '0022500001'


class TestSelectCandidateGames:
    def test_keeps_games_inside_window(self):
        game = NbaGame(
            gameId='0022500001',
            gameDateEst='2025-10-28T00:00:00Z',
            gameDateTimeEst='2025-10-28T19:30:00Z',
            gameStatus=1,
            gameStatusText='Scheduled',
            homeTeamId=1610612747,
            homeTeamName='Lakers',
            homeTeamTricode='LAL',
            awayTeamId=1610612738,
            awayTeamName='Celtics',
            awayTeamTricode='BOS',
        )
        now_utc = datetime(2025, 10, 25, tzinfo=timezone.utc)
        candidates = select_candidate_games([game], now_utc=now_utc, refresh_days=14)
        assert len(candidates) == 1

    def test_excludes_past_games_even_if_non_final(self):
        game = NbaGame(
            gameId='0022500001',
            gameDateEst='2025-01-01T00:00:00Z',
            gameDateTimeEst='2025-01-01T00:00:00Z',
            gameStatus=2,
            gameStatusText='Q3',
            homeTeamId=1610612747,
            homeTeamName='Lakers',
            homeTeamTricode='LAL',
            awayTeamId=1610612738,
            awayTeamName='Celtics',
            awayTeamTricode='BOS',
        )
        now_utc = datetime(2025, 10, 25, tzinfo=timezone.utc)
        candidates = select_candidate_games([game], now_utc=now_utc, refresh_days=14)
        assert len(candidates) == 0

    def test_excludes_future_games_beyond_horizon(self):
        game = NbaGame(
            gameId='0022500002',
            gameDateEst='2025-11-20T00:00:00Z',
            gameDateTimeEst='2025-11-20T00:00:00Z',
            gameStatus=1,
            gameStatusText='Scheduled',
            homeTeamId=1610612747,
            homeTeamName='Lakers',
            homeTeamTricode='LAL',
            awayTeamId=1610612738,
            awayTeamName='Celtics',
            awayTeamTricode='BOS',
        )
        now_utc = datetime(2025, 10, 25, tzinfo=timezone.utc)
        candidates = select_candidate_games([game], now_utc=now_utc, refresh_days=14)
        assert len(candidates) == 0

    def test_write_all_includes_full_mapped_season(self):
        final_game = NbaGame(
            gameId='0022500003',
            gameDateEst='2025-01-01T00:00:00Z',
            gameDateTimeEst='2025-01-01T00:00:00Z',
            gameStatus=3,
            gameStatusText='Final',
            homeTeamId=1610612747,
            homeTeamName='Lakers',
            homeTeamTricode='LAL',
            awayTeamId=1610612738,
            awayTeamName='Celtics',
            awayTeamTricode='BOS',
        )
        future_game = NbaGame(
            gameId='0022500004',
            gameDateEst='2030-01-01T00:00:00Z',
            gameDateTimeEst='2030-01-01T00:00:00Z',
            gameStatus=1,
            gameStatusText='Scheduled',
            homeTeamId=1610612747,
            homeTeamName='Lakers',
            homeTeamTricode='LAL',
            awayTeamId=1610612738,
            awayTeamName='Celtics',
            awayTeamTricode='BOS',
        )
        now_utc = datetime(2025, 10, 25, tzinfo=timezone.utc)
        candidates = select_candidate_games(
            [final_game, future_game],
            now_utc=now_utc,
            refresh_days=14,
            write_all_season_games=True,
        )
        assert len(candidates) == 2

    def test_from_date_with_include_final_includes_replay_range(self):
        final_game = NbaGame(
            gameId='0022500003',
            gameDateEst='2025-03-01T00:00:00Z',
            gameDateTimeEst='2025-03-01T00:00:00Z',
            gameStatus=3,
            gameStatusText='Final',
            homeTeamId=1610612747,
            homeTeamName='Lakers',
            homeTeamTricode='LAL',
            awayTeamId=1610612738,
            awayTeamName='Celtics',
            awayTeamTricode='BOS',
        )
        now_utc = datetime(2025, 10, 25, tzinfo=timezone.utc)
        candidates = select_candidate_games(
            [final_game],
            now_utc=now_utc,
            refresh_days=14,
            from_date_utc='2025-02-01T00:00:00Z',
            include_final_games=True,
        )
        assert len(candidates) == 1

    def test_from_date_to_date_bounds_the_replay_range(self):
        in_range = NbaGame(
            gameId='0022500010',
            gameDateEst='2025-03-05T00:00:00Z',
            gameDateTimeEst='2025-03-05T00:00:00Z',
            gameStatus=2,
            gameStatusText='Q4',
            homeTeamId=1610612747,
            homeTeamName='Lakers',
            homeTeamTricode='LAL',
            awayTeamId=1610612738,
            awayTeamName='Celtics',
            awayTeamTricode='BOS',
        )
        after_range = NbaGame(
            gameId='0022500011',
            gameDateEst='2025-03-20T00:00:00Z',
            gameDateTimeEst='2025-03-20T00:00:00Z',
            gameStatus=2,
            gameStatusText='Q4',
            homeTeamId=1610612747,
            homeTeamName='Lakers',
            homeTeamTricode='LAL',
            awayTeamId=1610612738,
            awayTeamName='Celtics',
            awayTeamTricode='BOS',
        )
        now_utc = datetime(2025, 10, 25, tzinfo=timezone.utc)
        candidates = select_candidate_games(
            [in_range, after_range],
            now_utc=now_utc,
            refresh_days=14,
            from_date_utc='2025-03-01T00:00:00Z',
            to_date_utc='2025-03-10T23:59:59Z',
        )
        assert len(candidates) == 1
        assert candidates[0].gameId == '0022500010'

    def test_from_date_until_default_horizon(self):
        in_range = NbaGame(
            gameId='0022500012',
            gameDateEst='2025-10-30T00:00:00Z',
            gameDateTimeEst='2025-10-30T00:00:00Z',
            gameStatus=1,
            gameStatusText='Scheduled',
            homeTeamId=1610612747,
            homeTeamName='Lakers',
            homeTeamTricode='LAL',
            awayTeamId=1610612738,
            awayTeamName='Celtics',
            awayTeamTricode='BOS',
        )
        after_horizon = NbaGame(
            gameId='0022500013',
            gameDateEst='2025-11-20T00:00:00Z',
            gameDateTimeEst='2025-11-20T00:00:00Z',
            gameStatus=1,
            gameStatusText='Scheduled',
            homeTeamId=1610612747,
            homeTeamName='Lakers',
            homeTeamTricode='LAL',
            awayTeamId=1610612738,
            awayTeamName='Celtics',
            awayTeamTricode='BOS',
        )
        now_utc = datetime(2025, 10, 25, tzinfo=timezone.utc)
        candidates = select_candidate_games(
            [in_range, after_horizon],
            now_utc=now_utc,
            refresh_days=14,
            from_date_utc='2025-10-25T00:00:00Z',
            replay_until_default_horizon=True,
        )
        assert len(candidates) == 1
        assert candidates[0].gameId == '0022500012'


class TestGamesService:
    @pytest.fixture
    def mock_repository(self):
        repo = MagicMock()
        repo.upsert_changed_batch.return_value = 1
        return repo

    @pytest.fixture
    def service(self, mock_repository):
        return GamesService(repository=mock_repository)

    @patch('src.service.games_service._fetch_latest_schedule_document', return_value=RAW_DOCUMENT)
    @patch('src.service.games_service._get_bucket_name', return_value='test-bucket')
    @patch('src.service.games_service._get_refresh_days', return_value=14)
    def test_consume_games_returns_counters(self, mock_days, mock_bucket, mock_fetch, service):
        result = service.consume_games(input_options={'write_all_season_games': True})

        assert result['flattened_games'] == 2
        assert result['mapped_games'] == 1
        assert result['candidate_games'] == 1
        assert result['written_games'] == 1

    def test_consume_games_from_document_from_date_includes_finals(self, service):
        result = service.consume_games_from_document(
            RAW_DOCUMENT,
            input_options={
                'from_date_utc': '2000-01-01T00:00:00Z',
                'include_final_games': True,
            },
        )
        assert result['candidate_games'] == 1

    def test_rejects_to_date_without_from_date(self, service):
        with pytest.raises(ValueError, match='requires from_date_utc'):
            service.consume_games_from_document(
                RAW_DOCUMENT,
                input_options={'to_date_utc': '2025-03-10T00:00:00Z'},
            )

    def test_rejects_conflicting_replay_end_options(self, service):
        with pytest.raises(ValueError, match='mutually exclusive'):
            service.consume_games_from_document(
                RAW_DOCUMENT,
                input_options={
                    'from_date_utc': '2025-03-01T00:00:00Z',
                    'to_date_utc': '2025-03-10T00:00:00Z',
                    'replay_until_default_horizon': True,
                },
            )

    def test_rejects_invalid_date_range(self, service):
        with pytest.raises(ValueError, match='greater than or equal to'):
            service.consume_games_from_document(
                RAW_DOCUMENT,
                input_options={
                    'from_date_utc': '2025-03-10T00:00:00Z',
                    'to_date_utc': '2025-03-01T00:00:00Z',
                },
            )

    def test_rejects_unknown_type_for_boolean_input(self, service):
        with pytest.raises(ValueError, match='must be a boolean'):
            service.consume_games_from_document(
                RAW_DOCUMENT,
                input_options={'write_all_season_games': 'yes'},
            )

    @patch('src.service.games_service._get_bucket_name', side_effect=ValueError('S3_BUCKET_NAME not set'))
    def test_consume_games_raises_on_missing_bucket(self, mock_bucket, service):
        with pytest.raises(ValueError, match='S3_BUCKET_NAME not set'):
            service.consume_games()
