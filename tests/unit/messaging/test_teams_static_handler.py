"""
Unit tests for the teams-static handler with mocked service.
"""
import json
import pytest
from unittest.mock import MagicMock

from src.messaging.teams_static_handler import TeamsStaticHandler


class TestTeamsStaticHandlerUnit:
    """Unit tests for TeamsStaticHandler."""

    @pytest.fixture
    def mock_service(self):
        """Create a mock TeamsStaticService."""
        service = MagicMock()
        service.fetch_latest_teams_document.return_value = {
            'source': 'nba_api',
            'endpoint': 'teams_static',
            'fetched_at_utc': '2026-03-08T13:09:01.492795+00:00Z',
            'aws_account_id': '590183661886',
            'params': {},
            'payload': [
                {
                    'id': 1610612737,
                    'full_name': 'Atlanta Hawks',
                    'abbreviation': 'ATL',
                    'nickname': 'Hawks',
                    'city': 'Atlanta',
                    'state': 'Georgia',
                    'year_founded': 1949,
                }
            ],
        }
        service.consume_teams_from_document.return_value = 30
        return service

    @pytest.fixture
    def handler(self, mock_service):
        """Create handler with mocked service."""
        return TeamsStaticHandler(service=mock_service)

    def test_handle_success(self, handler, mock_service):
        """handle() should return 200 with team count."""
        response = handler.handle()

        assert response['statusCode'] == 200
        body = json.loads(response['body'])
        assert body['message'] == 'Teams consumed and persisted successfully'
        assert body['teams_persisted'] == 30
        mock_service.fetch_latest_teams_document.assert_called_once()
        mock_service.consume_teams_from_document.assert_called_once()

    def test_handle_invalid_raw_schema_returns_400(self, handler, mock_service):
        """Invalid raw document schema should return 400."""
        mock_service.fetch_latest_teams_document.return_value = {
            'source': 'nba_api',
            'payload': [],
        }
        response = handler.handle()

        assert response['statusCode'] == 400
        body = json.loads(response['body'])
        assert 'Validation error' in body['error']
        mock_service.consume_teams_from_document.assert_not_called()

    def test_handle_file_not_found_returns_422(self, handler, mock_service):
        """FileNotFoundError from service should return 422."""
        mock_service.fetch_latest_teams_document.side_effect = FileNotFoundError("No S3 data")
        response = handler.handle()

        assert response['statusCode'] == 422
        body = json.loads(response['body'])
        assert 'No S3 data' in body['error']

    def test_handle_value_error_returns_422(self, handler, mock_service):
        """ValueError from service should return 422."""
        mock_service.fetch_latest_teams_document.side_effect = ValueError("S3_BUCKET_NAME not set")
        response = handler.handle()

        assert response['statusCode'] == 422

    def test_handle_unexpected_exception_returns_500(self, handler, mock_service):
        """Unexpected exception should return 500."""
        mock_service.fetch_latest_teams_document.side_effect = RuntimeError("Something broke")
        response = handler.handle()

        assert response['statusCode'] == 500
        body = json.loads(response['body'])
        assert body['error'] == 'Internal server error'
