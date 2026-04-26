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
            'fetched_at_utc': '2026-04-26T13:00:00Z',
            'aws_account_id': '590183661886',
            'params': {},
            'payload': {
                'resource': 'playertindex',
                'parameters': {},
                'resultSets': [
                    {
                        'name': 'PlayerIndex',
                        'headers': [
                            'PERSON_ID',
                            'PLAYER_FIRST_NAME',
                            'PLAYER_LAST_NAME',
                            'POSITION',
                            'JERSEY_NUMBER',
                            'HEIGHT',
                            'COUNTRY',
                            'ROSTER_STATUS',
                        ],
                        'rowSet': [
                            [
                                2544,
                                'LeBron',
                                'James',
                                'F',
                                23,
                                '6-9',
                                'USA',
                                1,
                            ],
                            [
                                201939,
                                'Stephen',
                                'Curry',
                                'G',
                                30,
                                '6-2',
                                'USA',
                                1,
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
