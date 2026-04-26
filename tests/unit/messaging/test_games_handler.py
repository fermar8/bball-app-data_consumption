"""
Unit tests for the games handler with mocked service.
"""
import json
from unittest.mock import MagicMock

import pytest

from src.messaging.games_handler import GamesHandler


class TestGamesHandlerUnit:
    @pytest.fixture
    def mock_service(self):
        service = MagicMock()
        service.fetch_latest_schedule_document.return_value = {
            'source': 'nba_api',
            'endpoint': 'schedule_league_v2',
            'fetched_at_utc': '2026-03-08T13:09:01.492795+00:00Z',
            'aws_account_id': '590183661886',
            'params': {},
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
                                    'gameDateEst': '2025-10-22T00:00:00Z',
                                    'gameDateTimeEst': '2025-10-22T19:30:00Z',
                                    'gameLabel': 'Regular Season',
                                    'homeTeam': {
                                        'teamId': 1610612747,
                                        'teamName': 'Lakers',
                                        'teamTricode': 'LAL',
                                    },
                                    'awayTeam': {
                                        'teamId': 1610612738,
                                        'teamName': 'Celtics',
                                        'teamTricode': 'BOS',
                                    },
                                }
                            ],
                        }
                    ]
                }
            },
        }
        service.consume_games_from_document.return_value = {
            'flattened_games': 1,
            'mapped_games': 1,
            'candidate_games': 1,
            'written_games': 1,
        }
        return service

    @pytest.fixture
    def handler(self, mock_service):
        return GamesHandler(service=mock_service)

    def test_handle_success(self, handler, mock_service):
        response = handler.handle()

        assert response['statusCode'] == 200
        body = json.loads(response['body'])
        assert body['message'] == 'Games consumed and persisted successfully'
        assert body['written_games'] == 1
        mock_service.fetch_latest_schedule_document.assert_called_once()
        mock_service.consume_games_from_document.assert_called_once_with(
            mock_service.fetch_latest_schedule_document.return_value,
            input_options={},
        )

    def test_handle_forwards_runtime_input_options(self, handler, mock_service):
        event = {
            'input': {
                'write_all_season_games': True,
                'include_final_games': True,
                'from_date_utc': '2026-03-01T00:00:00Z',
                'to_date_utc': '2026-03-10T00:00:00Z',
                'replay_until_default_horizon': False,
            }
        }
        response = handler.handle(event=event)

        assert response['statusCode'] == 200
        mock_service.consume_games_from_document.assert_called_once_with(
            mock_service.fetch_latest_schedule_document.return_value,
            input_options={
                'write_all_season_games': True,
                'include_final_games': True,
                'from_date_utc': '2026-03-01T00:00:00Z',
                'to_date_utc': '2026-03-10T00:00:00Z',
                'replay_until_default_horizon': False,
            },
        )

    def test_handle_rejects_unknown_manual_input_keys(self, handler):
        response = handler.handle(event={'input': {'unexpected_option': True}})
        assert response['statusCode'] == 422
        body = json.loads(response['body'])
        assert 'Unknown input option' in body['error']

    def test_handle_invalid_raw_schema_returns_400(self, handler, mock_service):
        mock_service.fetch_latest_schedule_document.return_value = {
            'source': 'nba_api',
            'payload': {},
        }
        response = handler.handle()

        assert response['statusCode'] == 400
        body = json.loads(response['body'])
        assert 'Validation error' in body['error']
        mock_service.consume_games_from_document.assert_not_called()

    def test_handle_file_not_found_returns_422(self, handler, mock_service):
        mock_service.fetch_latest_schedule_document.side_effect = FileNotFoundError('No S3 data')
        response = handler.handle()

        assert response['statusCode'] == 422
        body = json.loads(response['body'])
        assert 'No S3 data' in body['error']

    def test_handle_unexpected_exception_returns_500(self, handler, mock_service):
        mock_service.fetch_latest_schedule_document.side_effect = RuntimeError('Something broke')
        response = handler.handle()

        assert response['statusCode'] == 500
        body = json.loads(response['body'])
        assert body['error'] == 'Internal server error'
