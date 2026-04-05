"""
Unit tests for the messaging/handler layer with mocked service.
"""
import json
import pytest
from unittest.mock import MagicMock

from src.messaging.handler import Handler


class TestHandlerUnit:
    """Unit tests for Handler."""

    @pytest.fixture
    def mock_service(self):
        """Create a mock TeamsConsumptionService."""
        service = MagicMock()
        service.consume_teams.return_value = 30
        return service

    @pytest.fixture
    def handler(self, mock_service):
        """Create handler with mocked service."""
        return Handler(service=mock_service)

    def test_handle_consume_teams_static_success(self, handler, mock_service):
        """Valid consume_teams_static event should return 200 with team count."""
        event = {'action': 'consume_teams_static'}
        response = handler.handle(event)

        assert response['statusCode'] == 200
        body = json.loads(response['body'])
        assert body['message'] == 'Teams consumed and persisted successfully'
        assert body['teams_persisted'] == 30
        mock_service.consume_teams.assert_called_once()

    def test_handle_missing_action_fails_validation(self, handler, mock_service):
        """Event without action should fail schema validation."""
        event = {}
        response = handler.handle(event)

        assert response['statusCode'] == 400
        body = json.loads(response['body'])
        assert 'Validation error' in body['error']
        mock_service.consume_teams.assert_not_called()

    def test_handle_invalid_action_fails_validation(self, handler, mock_service):
        """Unknown action should fail schema validation."""
        event = {'action': 'do_something_else'}
        response = handler.handle(event)

        assert response['statusCode'] == 400
        body = json.loads(response['body'])
        assert 'Validation error' in body['error']
        mock_service.consume_teams.assert_not_called()

    def test_handle_extra_fields_fail_validation(self, handler, mock_service):
        """Extra fields should fail schema validation (additionalProperties: false)."""
        event = {'action': 'consume_teams_static', 'extra': 'field'}
        response = handler.handle(event)

        assert response['statusCode'] == 400
        body = json.loads(response['body'])
        assert 'Validation error' in body['error']
        mock_service.consume_teams.assert_not_called()

    def test_handle_file_not_found_returns_422(self, handler, mock_service):
        """FileNotFoundError from service should return 422."""
        mock_service.consume_teams.side_effect = FileNotFoundError("No S3 data")
        event = {'action': 'consume_teams_static'}
        response = handler.handle(event)

        assert response['statusCode'] == 422
        body = json.loads(response['body'])
        assert 'No S3 data' in body['error']

    def test_handle_value_error_returns_422(self, handler, mock_service):
        """ValueError from service should return 422."""
        mock_service.consume_teams.side_effect = ValueError("S3_BUCKET_NAME not set")
        event = {'action': 'consume_teams_static'}
        response = handler.handle(event)

        assert response['statusCode'] == 422

    def test_handle_unexpected_exception_returns_500(self, handler, mock_service):
        """Unexpected exception should return 500."""
        mock_service.consume_teams.side_effect = RuntimeError("Something broke")
        event = {'action': 'consume_teams_static'}
        response = handler.handle(event)

        assert response['statusCode'] == 500
        body = json.loads(response['body'])
        assert body['error'] == 'Internal server error'
