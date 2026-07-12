"""
Unit tests for the player game stats handler with mocked service.
"""
import json
from unittest.mock import MagicMock

import pytest

from src.messaging.players_game_stats_handler import PlayersGameStatsHandler


class TestPlayersGameStatsHandlerUnit:
    @pytest.fixture
    def mock_service(self):
        service = MagicMock()
        service.fetch_latest_player_game_logs_document.return_value = {
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
                            'SEASON_YEAR',
                            'PLAYER_ID',
                            'PLAYER_NAME',
                            'NICKNAME',
                            'TEAM_ID',
                            'TEAM_ABBREVIATION',
                            'TEAM_NAME',
                            'GAME_ID',
                            'GAME_DATE',
                            'MATCHUP',
                            'WL',
                            'MIN',
                            'FGM',
                            'FGA',
                            'FG_PCT',
                            'FG3M',
                            'FG3A',
                            'FG3_PCT',
                            'FTM',
                            'FTA',
                            'FT_PCT',
                            'OREB',
                            'DREB',
                            'REB',
                            'AST',
                            'TOV',
                            'STL',
                            'BLK',
                            'BLKA',
                            'PF',
                            'PFD',
                            'PTS',
                            'PLUS_MINUS',
                            'NBA_FANTASY_PTS',
                            'DD2',
                            'TD3',
                            'WNBA_FANTASY_PTS',
                            'AVAILABLE_FLAG',
                            'MIN_SEC',
                            'TEAM_COUNT',
                        ],
                        'rowSet': [
                            [
                                '2025-26',
                                1630552,
                                'Jalen Johnson',
                                'Jalen',
                                1610612737,
                                'ATL',
                                'Atlanta Hawks',
                                '0022500917',
                                '2026-03-07T00:00:00',
                                'ATL vs. PHI',
                                'W',
                                '38:40',
                                12,
                                19,
                                0.632,
                                2,
                                4,
                                0.5,
                                9,
                                9,
                                1.0,
                                2,
                                8,
                                10,
                                7,
                                5,
                                2,
                                0,
                                1,
                                2,
                                6,
                                35,
                                12,
                                58.5,
                                1,
                                0,
                                58.0,
                                1,
                                '38:40',
                                1,
                            ]
                        ],
                    }
                ],
            },
        }
        service.consume_player_game_logs_from_document.return_value = {
            'flattened_player_game_logs': 1,
            'mapped_player_game_logs': 1,
            'written_player_game_logs': 1,
        }
        return service

    @pytest.fixture
    def handler(self, mock_service):
        return PlayersGameStatsHandler(service=mock_service)

    def test_handle_success(self, handler, mock_service):
        response = handler.handle()

        assert response['statusCode'] == 200
        body = json.loads(response['body'])
        assert body['message'] == 'Player game logs consumed and persisted successfully'
        assert body['written_player_game_logs'] == 1
        mock_service.fetch_latest_player_game_logs_document.assert_called_once()
        mock_service.consume_player_game_logs_from_document.assert_called_once_with(
            mock_service.fetch_latest_player_game_logs_document.return_value,
        )

    def test_handle_invalid_raw_schema_returns_400(self, handler, mock_service):
        mock_service.fetch_latest_player_game_logs_document.return_value = {
            'source': 'nba_api',
            'payload': {},
        }
        response = handler.handle()

        assert response['statusCode'] == 400
        body = json.loads(response['body'])
        assert 'Validation error' in body['error']
        mock_service.consume_player_game_logs_from_document.assert_not_called()

    def test_handle_file_not_found_returns_422(self, handler, mock_service):
        mock_service.fetch_latest_player_game_logs_document.side_effect = FileNotFoundError(
            'No S3 data'
        )
        response = handler.handle()

        assert response['statusCode'] == 422
        body = json.loads(response['body'])
        assert 'No S3 data' in body['error']

    def test_handle_unexpected_exception_returns_500(self, handler, mock_service):
        mock_service.fetch_latest_player_game_logs_document.side_effect = RuntimeError('Boom')
        response = handler.handle()

        assert response['statusCode'] == 500
        body = json.loads(response['body'])
        assert body['error'] == 'Internal server error'

    def test_handle_accepts_full_rank_headers_and_numeric_min(self, handler, mock_service):
        headers = [
            'SEASON_YEAR', 'PLAYER_ID', 'PLAYER_NAME', 'NICKNAME', 'TEAM_ID',
            'TEAM_ABBREVIATION', 'TEAM_NAME', 'GAME_ID', 'GAME_DATE', 'MATCHUP', 'WL',
            'MIN', 'FGM', 'FGA', 'FG_PCT', 'FG3M', 'FG3A', 'FG3_PCT', 'FTM', 'FTA',
            'FT_PCT', 'OREB', 'DREB', 'REB', 'AST', 'TOV', 'STL', 'BLK', 'BLKA', 'PF',
            'PFD', 'PTS', 'PLUS_MINUS', 'NBA_FANTASY_PTS', 'DD2', 'TD3',
            'WNBA_FANTASY_PTS', 'GP_RANK', 'W_RANK', 'L_RANK', 'W_PCT_RANK', 'MIN_RANK',
            'FGM_RANK', 'FGA_RANK', 'FG_PCT_RANK', 'FG3M_RANK', 'FG3A_RANK',
            'FG3_PCT_RANK', 'FTM_RANK', 'FTA_RANK', 'FT_PCT_RANK', 'OREB_RANK',
            'DREB_RANK', 'REB_RANK', 'AST_RANK', 'TOV_RANK', 'STL_RANK', 'BLK_RANK',
            'BLKA_RANK', 'PF_RANK', 'PFD_RANK', 'PTS_RANK', 'PLUS_MINUS_RANK',
            'NBA_FANTASY_PTS_RANK', 'DD2_RANK', 'TD3_RANK', 'WNBA_FANTASY_PTS_RANK',
            'AVAILABLE_FLAG', 'MIN_SEC', 'TEAM_COUNT'
        ]
        row = [
            '2025-26', 1630552, 'Jalen Johnson', 'Jalen', 1610612737, 'ATL',
            'Atlanta Hawks', '0022500917', '2026-03-07T00:00:00', 'ATL vs. PHI', 'W',
            38.666666666666664, 12, 19, 0.632, 2, 4, 0.5, 9, 9, 1.0, 2, 8, 10, 7, 5, 2,
            0, 1, 2, 6, 35, 12, 58.5, 1, 0, 58.0, 1, 1, 1, 1, 5, 1, 4, 20, 21, 38, 15,
            7, 8, 1, 17, 5, 11, 5, 3, 8, 41, 86, 56, 7, 1, 16, 2, 1, 1, 1, 1, '38:40',
            1
        ]

        document = mock_service.fetch_latest_player_game_logs_document.return_value
        document['payload']['resultSets'][0]['headers'] = headers
        document['payload']['resultSets'][0]['rowSet'] = [row]

        response = handler.handle()

        assert response['statusCode'] == 200
        body = json.loads(response['body'])
        assert body['written_player_game_logs'] == 1
