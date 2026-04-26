"""
Unit tests for the players_index service layer with mocked dependencies.
"""
from unittest.mock import MagicMock

import pytest

from src.model.models import NbaPlayer
from src.service.players_index_service import (
    PlayersIndexService,
    extract_players_from_payload,
    map_players,
)


RAW_DOCUMENT = {
    'payload': {
        'resource': 'playerindex',
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
                    [2544, 'LeBron', 'James', 'F', 23, '6-9', 'USA', 1],
                    [201939, 'Stephen', 'Curry', 'G', 30, '6-2', 'USA', 1],
                    [2025, 'Invalid', 'Player', None, None, None, None, None],
                ],
            }
        ],
    }
}


class TestExtractPlayersFromPayload:
    def test_extracts_players_from_payload(self):
        players = extract_players_from_payload(RAW_DOCUMENT)
        assert len(players) == 3
        assert players[0]['PERSON_ID'] == 2544
        assert players[1]['PERSON_ID'] == 201939

    def test_raises_when_missing_result_sets(self):
        with pytest.raises(ValueError, match='resultSets'):
            extract_players_from_payload({'payload': {}})

    def test_raises_when_no_player_index_set(self):
        with pytest.raises(ValueError, match='PlayerIndex'):
            extract_players_from_payload(
                {'payload': {'resultSets': [{'name': 'OtherSet', 'headers': [], 'rowSet': []}]}}
            )

    def test_raises_when_missing_headers(self):
        with pytest.raises(ValueError, match='headers'):
            extract_players_from_payload(
                {'payload': {'resultSets': [{'name': 'PlayerIndex', 'headers': [], 'rowSet': []}]}}
            )


class TestMapPlayers:
    def test_maps_valid_players(self):
        raw_players = extract_players_from_payload(RAW_DOCUMENT)
        mapped = map_players(raw_players)
        
        assert len(mapped) == 2  # Skips invalid player
        assert isinstance(mapped[0], NbaPlayer)
        assert mapped[0].playerId == 2544
        assert mapped[0].firstName == 'LeBron'
        assert mapped[1].playerId == 201939
        assert mapped[1].firstName == 'Stephen'

    def test_skips_malformed_players(self):
        # The invalid player (with None values) should be skipped
        raw_players = extract_players_from_payload(RAW_DOCUMENT)
        mapped = map_players(raw_players)
        
        assert len(mapped) == 2


class TestPlayersIndexService:
    @pytest.fixture
    def mock_repository(self):
        repo = MagicMock()
        repo.upsert_changed_batch.return_value = 2
        return repo

    @pytest.fixture
    def service(self, mock_repository):
        return PlayersIndexService(repository=mock_repository)

    def test_consume_players_from_document(self, service, mock_repository):
        result = service.consume_players_from_document(RAW_DOCUMENT)

        assert result['flattened_players'] == 3
        assert result['mapped_players'] == 2
        assert result['written_players'] == 2
        mock_repository.upsert_changed_batch.assert_called_once()

    def test_consume_players_calls_repository(self, service, mock_repository):
        service.consume_players_from_document(RAW_DOCUMENT)

        mock_repository.upsert_changed_batch.assert_called_once()
        call_args = mock_repository.upsert_changed_batch.call_args[0][0]
        assert len(call_args) == 2
        assert all(isinstance(p, NbaPlayer) for p in call_args)
