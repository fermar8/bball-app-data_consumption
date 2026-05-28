"""
Unit tests for the players_index handler with mocked service.
"""
import json
from unittest.mock import MagicMock

import pytest

from src.messaging.players_index_handler import PlayersIndexHandler


class TestPlayersIndexHandlerUnit:
    @pytest.fixture
    def mock_service(self):
        service = MagicMock()
        service.fetch_latest_player_index_document.return_value = {
            'source': 'nba_api',
            'endpoint': 'player_index',
            'fetched_at_utc': '2026-03-10T11:40:09.005839+00:00',
            'aws_account_id': '590183661886',
            'schema_version': 'v1',
            'ingestion_id': '10beff59-807e-42da-b685-5f1ad63ec9e2',
            'params': {'active': 1},
            'payload': {
                'resource': 'playerindex',
                'parameters': {
                    'LeagueID': '00',
                    'Season': '2025-26',
                    'Historical': 0,
                    'TeamID': 0,
                    'Country': None,
                    'College': None,
                    'DraftYear': None,
                    'DraftPick': None,
                    'PlayerPosition': '',
                    'Height': None,
                    'Weight': None,
                    'Active': 1,
                    'AllStar': 0,
                },
                'resultSets': [
                    {
                        'name': 'PlayerIndex',
                        'headers': [
                            'PERSON_ID',
                            'PLAYER_LAST_NAME',
                            'PLAYER_FIRST_NAME',
                            'PLAYER_SLUG',
                            'TEAM_ID',
                            'TEAM_SLUG',
                            'IS_DEFUNCT',
                            'TEAM_CITY',
                            'TEAM_NAME',
                            'TEAM_ABBREVIATION',
                            'JERSEY_NUMBER',
                            'POSITION',
                            'HEIGHT',
                            'WEIGHT',
                            'COLLEGE',
                            'COUNTRY',
                            'DRAFT_YEAR',
                            'DRAFT_ROUND',
                            'DRAFT_NUMBER',
                            'ROSTER_STATUS',
                            'FROM_YEAR',
                            'TO_YEAR',
                            'PTS',
                            'REB',
                            'AST',
                            'STATS_TIMEFRAME',
                        ],
                        'rowSet': [
                            [
                                1630173,
                                'Achiuwa',
                                'Precious',
                                'precious-achiuwa',
                                1610612758,
                                'kings',
                                0,
                                'Sacramento',
                                'Kings',
                                'SAC',
                                '9',
                                'F',
                                '6-8',
                                '243',
                                'Memphis',
                                'Nigeria',
                                2020,
                                1,
                                20,
                                1,
                                '2020',
                                '2025',
                                8.7,
                                6.1,
                                1.3,
                                'Season',
                            ],
                            [
                                203500,
                                'Adams',
                                'Steven',
                                'steven-adams',
                                1610612745,
                                'rockets',
                                0,
                                'Houston',
                                'Rockets',
                                'HOU',
                                '12',
                                'C',
                                '6-11',
                                '265',
                                'Pittsburgh',
                                'New Zealand',
                                2013,
                                1,
                                12,
                                1,
                                '2013',
                                '2025',
                                5.8,
                                8.6,
                                1.5,
                                'Season',
                            ],
                        ],
                    }
                ],
            },
        }
        service.consume_players_from_document.return_value = {
            'flattened_players': 2,
            'mapped_players': 2,
            'written_players': 2,
        }
        return service

    @pytest.fixture
    def handler(self, mock_service):
        return PlayersIndexHandler(service=mock_service)

    def test_handle_success(self, handler, mock_service):
        response = handler.handle()

        assert response['statusCode'] == 200
        body = json.loads(response['body'])
        assert body['message'] == 'Players consumed and persisted successfully'
        assert body['written_players'] == 2
        mock_service.fetch_latest_player_index_document.assert_called_once()
        mock_service.consume_players_from_document.assert_called_once_with(
            mock_service.fetch_latest_player_index_document.return_value,
        )

    def test_handle_schema_validation_error(self, handler, mock_service):
        mock_service.fetch_latest_player_index_document.return_value = {
            'source': 'invalid_source',
            'endpoint': 'player_index',
        }

        response = handler.handle()

        assert response['statusCode'] == 400
        body = json.loads(response['body'])
        assert 'Validation error' in body['error']

    def test_handle_file_not_found_error(self, handler, mock_service):
        mock_service.fetch_latest_player_index_document.side_effect = FileNotFoundError(
            "No player_index files found"
        )

        response = handler.handle()

        assert response['statusCode'] == 422
        body = json.loads(response['body'])
        assert 'No player_index files found' in body['error']

    def test_handle_value_error(self, handler, mock_service):
        mock_service.fetch_latest_player_index_document.side_effect = ValueError(
            "Invalid data format"
        )

        response = handler.handle()

        assert response['statusCode'] == 422
        body = json.loads(response['body'])
        assert 'Invalid data format' in body['error']

    def test_handle_unexpected_error(self, handler, mock_service):
        mock_service.fetch_latest_player_index_document.side_effect = Exception(
            "Unexpected error"
        )

        response = handler.handle()

        assert response['statusCode'] == 500
        body = json.loads(response['body'])
        assert body['error'] == 'Internal server error'
