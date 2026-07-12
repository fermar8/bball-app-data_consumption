"""
Unit tests for the players_injuries handler with mocked service.
"""
import json
from unittest.mock import MagicMock

import pytest

from src.messaging.players_injuries_handler import PlayersInjuriesHandler


RAW_DOCUMENT = {
    'source': 'nba_com',
    'endpoint': 'injury_report',
    'fetched_at_utc': '2026-05-28T13:00:00+00:00',
    'aws_account_id': '590183661886',
    'schema_version': 'v1',
    'ingestion_id': 'abc-123',
    'params': {},
    'payload': {
        'source': 'nba_com',
        'raw_entries_count': 1,
        'count': 1,
        'updated_at': '2026-05-28T12:59:00+00:00',
        'injuries': [
            {
                'player_id': 2544,
                'player_name': 'LeBron James',
                'team_abbr': 'LAL',
                'status': 'out',
                'availability': 'no',
                'reason_type': 'injury',
                'reason': 'Left knee soreness',
                'report_date': '05/27/2026',
            }
        ],
    },
}


class TestPlayersInjuriesHandlerUnit:
    @pytest.fixture
    def mock_service(self):
        service = MagicMock()
        service.fetch_latest_injury_report_document.return_value = RAW_DOCUMENT
        service.consume_injuries_from_document.return_value = {
            'flattened_injuries': 1,
            'mapped_injuries': 1,
            'written_injuries': 1,
        }
        return service

    @pytest.fixture
    def handler(self, mock_service):
        return PlayersInjuriesHandler(service=mock_service)

    def test_handle_success(self, handler, mock_service):
        response = handler.handle()

        assert response['statusCode'] == 200
        body = json.loads(response['body'])
        assert body['message'] == 'Injuries consumed and persisted successfully'
        assert body['written_injuries'] == 1
        mock_service.fetch_latest_injury_report_document.assert_called_once()
        mock_service.consume_injuries_from_document.assert_called_once_with(RAW_DOCUMENT)

    def test_handle_schema_validation_error(self, handler, mock_service):
        mock_service.fetch_latest_injury_report_document.return_value = {
            'source': 'nba_com',
            'endpoint': 'wrong_endpoint',
        }

        response = handler.handle()

        assert response['statusCode'] == 400
        body = json.loads(response['body'])
        assert 'Validation error' in body['error']

    def test_handle_file_not_found_error(self, handler, mock_service):
        mock_service.fetch_latest_injury_report_document.side_effect = FileNotFoundError(
            "No injury_report files found"
        )

        response = handler.handle()

        assert response['statusCode'] == 422
        body = json.loads(response['body'])
        assert 'No injury_report files found' in body['error']

    def test_handle_value_error(self, handler, mock_service):
        mock_service.fetch_latest_injury_report_document.side_effect = ValueError(
            "Invalid data format"
        )

        response = handler.handle()

        assert response['statusCode'] == 422
        body = json.loads(response['body'])
        assert 'Invalid data format' in body['error']

    def test_handle_unexpected_error(self, handler, mock_service):
        mock_service.fetch_latest_injury_report_document.side_effect = Exception(
            "boom"
        )

        response = handler.handle()

        assert response['statusCode'] == 500
        body = json.loads(response['body'])
        assert body['error'] == 'Internal server error'

    def test_handle_accepts_empty_injuries_payload(self, handler, mock_service):
        mock_service.fetch_latest_injury_report_document.return_value = {
            'source': 'nba_com',
            'endpoint': 'injury_report',
            'fetched_at_utc': '2026-05-28T13:00:00+00:00',
            'aws_account_id': '590183661886',
            'schema_version': 'v1',
            'ingestion_id': 'abc-123',
            'params': {},
            'payload': {
                'source': 'nba_com',
                'raw_entries_count': 0,
                'count': 0,
                'updated_at': None,
                'injuries': [],
            },
        }
        mock_service.consume_injuries_from_document.return_value = {
            'flattened_injuries': 0,
            'mapped_injuries': 0,
            'written_injuries': 0,
        }

        response = handler.handle()

        assert response['statusCode'] == 200
        body = json.loads(response['body'])
        assert body['written_injuries'] == 0
