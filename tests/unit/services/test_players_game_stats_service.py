"""
Unit tests for the player game stats service layer with mocked dependencies.
"""
from unittest.mock import MagicMock, patch

import pytest

from src.model.models import NbaPlayerGameLog
from src.service.players_game_stats_service import (
    PlayersGameStatsService,
    extract_player_game_logs_from_payload,
    map_player_game_logs,
)


RAW_DOCUMENT = {
    'source': 'nba_api',
    'endpoint': 'player_game_logs',
    'fetched_at_utc': '2026-03-10T11:40:12.533658+00:00',
    'aws_account_id': '590183661886',
    'schema_version': 'v1',
    'ingestion_id': '63ced008-d185-48ac-92ee-6609d3d26b91',
    'params': {
        'season': '2025-26',
        'date_from': '03/07/2026',
        'date_to': '03/07/2026',
    },
    'payload': {
        'resource': 'gamelogs',
        'parameters': {
            'MeasureType': 'Base',
            'PerMode': 'Totals',
            'LeagueID': '00',
            'SeasonYear': '2025-26',
            'SeasonType': None,
            'PORound': None,
            'TeamID': 0,
            'PlayerID': 0,
            'Outcome': None,
            'Location': None,
            'Month': None,
            'SeasonSegment': None,
            'DateFrom': '03/07/2026',
            'DateTo': '03/07/2026',
            'OppTeamID': None,
            'VsConference': None,
            'VsDivision': None,
            'GameSegment': None,
            'Period': None,
            'ShotClockRange': None,
            'LastNGames': None,
            'ISTRound': None,
        },
        'resultSets': [
            {
                'name': 'PlayerGameLogs',
                'headers': [
                    'SEASON_YEAR', 'PLAYER_ID', 'PLAYER_NAME', 'NICKNAME', 'TEAM_ID',
                    'TEAM_ABBREVIATION', 'TEAM_NAME', 'GAME_ID', 'GAME_DATE', 'MATCHUP',
                    'WL', 'MIN', 'FGM', 'FGA', 'FG_PCT', 'FG3M', 'FG3A', 'FG3_PCT',
                    'FTM', 'FTA', 'FT_PCT', 'OREB', 'DREB', 'REB', 'AST', 'TOV', 'STL',
                    'BLK', 'BLKA', 'PF', 'PFD', 'PTS', 'PLUS_MINUS', 'NBA_FANTASY_PTS',
                    'DD2', 'TD3', 'WNBA_FANTASY_PTS', 'AVAILABLE_FLAG', 'MIN_SEC', 'TEAM_COUNT'
                ],
                'rowSet': [
                    [
                        '2025-26', 1630552, 'Jalen Johnson', 'Jalen', 1610612737, 'ATL',
                        'Atlanta Hawks', '0022500917', '2026-03-07T00:00:00', 'ATL vs. PHI',
                        'W', '38:40', 12, 19, 0.632, 2, 4, 0.5, 9, 9, 1.0, 2, 8, 10, 7,
                        5, 2, 0, 1, 2, 6, 35, 12, 58.5, 1, 0, 58.0, 1, '38:40', 1
                    ],
                    [
                        '2025-26', 1629008, 'Michael Porter Jr.', 'Michael', 1610612751, 'BKN',
                        'Brooklyn Nets', '0022500916', '2026-03-07T00:00:00', 'BKN @ DET',
                        'W', '39:17', 10, 25, 0.4, 3, 12, 0.25, 7, 7, 1.0, 2, 11, 13, 1,
                        2, 2, 1, 1, 2, 9, 30, 13, 54.1, 1, 0, 53.0, 1, '39:17', 1
                    ],
                    [
                        '2025-26', None, 'Bad Player', 'Bad', 1610612737, 'ATL', 'Atlanta Hawks',
                        None, '2026-03-07T00:00:00', 'ATL vs. PHI', 'W', '00:00', 0, 0, None, 0,
                        0, None, 0, 0, None, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, None, 0, 0,
                        None, 0, '00:00', 1
                    ],
                ],
            }
        ],
    },
}


class TestExtractPlayerGameLogsFromPayload:
    def test_extracts_player_game_logs_from_payload(self):
        rows = extract_player_game_logs_from_payload(RAW_DOCUMENT)
        assert len(rows) == 3
        assert rows[0]['PLAYER_ID'] == 1630552
        assert rows[1]['GAME_ID'] == '0022500916'

    def test_raises_when_missing_result_sets(self):
        with pytest.raises(ValueError, match='resultSets'):
            extract_player_game_logs_from_payload({'payload': {}})

    def test_raises_when_no_player_game_logs_set(self):
        with pytest.raises(ValueError, match='PlayerGameLogs'):
            extract_player_game_logs_from_payload(
                {'payload': {'resultSets': [{'name': 'OtherSet', 'headers': [], 'rowSet': []}]}}
            )

    def test_raises_when_missing_headers(self):
        with pytest.raises(ValueError, match='headers'):
            extract_player_game_logs_from_payload(
                {'payload': {'resultSets': [{'name': 'PlayerGameLogs', 'headers': [], 'rowSet': []}]}}
            )


class TestMapPlayerGameLogs:
    def test_maps_valid_player_game_logs(self):
        raw_rows = extract_player_game_logs_from_payload(RAW_DOCUMENT)
        mapped = map_player_game_logs(raw_rows)

        assert len(mapped) == 2
        assert isinstance(mapped[0], NbaPlayerGameLog)
        assert mapped[0].playerId == 1630552
        assert mapped[0].gameId == '0022500917'
        assert mapped[0].gameDate == '2026-03-07'
        assert mapped[0].pts == 35

    def test_skips_malformed_player_game_logs(self):
        raw_rows = extract_player_game_logs_from_payload(RAW_DOCUMENT)
        mapped = map_player_game_logs(raw_rows)

        assert len(mapped) == 2


class TestPlayersGameStatsService:
    @pytest.fixture
    def mock_repository(self):
        repo = MagicMock()
        repo.upsert_changed_batch.return_value = 2
        return repo

    @pytest.fixture
    def service(self, mock_repository):
        return PlayersGameStatsService(repository=mock_repository)

    def test_consume_player_game_logs_from_document(self, service, mock_repository):
        result = service.consume_player_game_logs_from_document(RAW_DOCUMENT)

        assert result['flattened_player_game_logs'] == 3
        assert result['mapped_player_game_logs'] == 2
        assert result['written_player_game_logs'] == 2
        mock_repository.upsert_changed_batch.assert_called_once()

    def test_consume_player_game_logs_calls_repository(self, service, mock_repository):
        service.consume_player_game_logs_from_document(RAW_DOCUMENT)

        mock_repository.upsert_changed_batch.assert_called_once()
        call_args = mock_repository.upsert_changed_batch.call_args[0][0]
        assert len(call_args) == 2
        assert all(isinstance(item, NbaPlayerGameLog) for item in call_args)

    @patch('src.service.players_game_stats_service._get_bucket_name', side_effect=ValueError('S3_BUCKET_NAME not set'))
    def test_consume_player_game_logs_raises_on_missing_bucket(self, mock_bucket, service):
        with pytest.raises(ValueError, match='S3_BUCKET_NAME not set'):
            service.consume_player_game_logs()
