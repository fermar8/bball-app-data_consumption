"""
Unit tests for the players_injuries service layer with mocked dependencies.
"""
from unittest.mock import MagicMock

import pytest

from src.model.models import NbaPlayerInjury
from src.service.players_injuries_service import (
    PlayersInjuriesService,
    extract_injuries_from_payload,
    map_injuries,
)


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
        'raw_entries_count': 3,
        'count': 3,
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
            },
            {
                'player_id': 203954,
                'player_name': 'Joel Embiid',
                'team_abbr': 'PHI',
                'status': 'questionable',
                'availability': 'doubtful',
                'reason_type': 'injury',
                'reason': 'Left knee',
                'report_date': '05/27/2026',
            },
            {
                'player_id': None,
                'player_name': 'Unknown Player',
                'team_abbr': 'LA Clippers',
                'status': 'available',
                'availability': 'yes',
                'reason_type': 'unknown',
                'reason': '',
                'report_date': '05/27/2026',
            },
        ],
    },
}


class TestExtractInjuriesFromPayload:
    def test_extracts_all_injury_entries(self):
        injuries = extract_injuries_from_payload(RAW_DOCUMENT)
        assert len(injuries) == 3
        assert injuries[0]['player_id'] == 2544

    def test_extracts_empty_list_when_no_injuries(self):
        doc = {'payload': {'injuries': []}}
        assert extract_injuries_from_payload(doc) == []

    def test_raises_when_missing_payload(self):
        with pytest.raises(ValueError, match='payload'):
            extract_injuries_from_payload({})

    def test_raises_when_missing_injuries_field(self):
        with pytest.raises(ValueError, match='injuries'):
            extract_injuries_from_payload({'payload': {}})


class TestMapInjuries:
    def test_maps_valid_injuries_and_skips_null_player_id(self):
        raw_injuries = extract_injuries_from_payload(RAW_DOCUMENT)
        mapped = map_injuries(
            raw_injuries,
            fetched_at='2026-05-28T13:00:00+00:00',
            updated_at='2026-05-28T12:59:00+00:00',
        )
        assert len(mapped) == 2
        assert all(isinstance(i, NbaPlayerInjury) for i in mapped)
        assert mapped[0].playerId == 2544
        assert mapped[1].playerId == 203954
        assert mapped[0].updatedAt == '2026-05-28T12:59:00+00:00'

    def test_returns_empty_list_when_no_injuries(self):
        assert map_injuries([], fetched_at='2026-05-28T13:00:00+00:00') == []


class TestPlayersInjuriesService:
    @pytest.fixture
    def mock_repository(self):
        repo = MagicMock()
        repo.upsert_changed_batch.return_value = 2
        return repo

    @pytest.fixture
    def service(self, mock_repository):
        return PlayersInjuriesService(repository=mock_repository)

    def test_consume_injuries_from_document(self, service, mock_repository):
        result = service.consume_injuries_from_document(RAW_DOCUMENT)

        assert result['flattened_injuries'] == 3
        assert result['mapped_injuries'] == 2
        assert result['written_injuries'] == 2
        mock_repository.upsert_changed_batch.assert_called_once()

    def test_consume_injuries_calls_repository_with_models(self, service, mock_repository):
        service.consume_injuries_from_document(RAW_DOCUMENT)

        call_args = mock_repository.upsert_changed_batch.call_args[0][0]
        assert len(call_args) == 2
        assert all(isinstance(i, NbaPlayerInjury) for i in call_args)

    def test_consume_handles_empty_injuries_list(self, service, mock_repository):
        mock_repository.upsert_changed_batch.return_value = 0
        doc = {
            'fetched_at_utc': '2026-05-28T13:00:00+00:00',
            'payload': {'updated_at': None, 'injuries': []},
        }
        result = service.consume_injuries_from_document(doc)
        assert result == {
            'flattened_injuries': 0,
            'mapped_injuries': 0,
            'written_injuries': 0,
        }
