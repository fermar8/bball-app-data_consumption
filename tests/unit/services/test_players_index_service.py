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
                        1630173, 'Achiuwa', 'Precious', 'precious-achiuwa',
                        1610612758, 'kings', 0, 'Sacramento', 'Kings', 'SAC',
                        '9', 'F', '6-8', '243', 'Memphis', 'Nigeria',
                        2020, 1, 20, 1, '2020', '2025', 8.7, 6.1, 1.3, 'Season',
                    ],
                    [
                        203500, 'Adams', 'Steven', 'steven-adams',
                        1610612745, 'rockets', 0, 'Houston', 'Rockets', 'HOU',
                        '12', 'C', '6-11', '265', 'Pittsburgh', 'New Zealand',
                        2013, 1, 12, 1, '2013', '2025', 5.8, 8.6, 1.5, 'Season',
                    ],
                    [
                        None, 'Invalid', 'Player', 'invalid-player',
                        1610612737, 'hawks', 0, 'Atlanta', 'Hawks', 'ATL',
                        None, None, None, None, None, None,
                        None, None, None, 1, None, None, None, None, None, 'Season',
                    ],
                ],
            }
        ],
    }
}


class TestExtractPlayersFromPayload:
    def test_extracts_players_from_payload(self):
        players = extract_players_from_payload(RAW_DOCUMENT)
        assert len(players) == 3
        assert players[0]['PERSON_ID'] == 1630173
        assert players[1]['PERSON_ID'] == 203500

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
        assert mapped[0].playerId == 1630173
        assert mapped[0].firstName == 'Precious'
        assert mapped[1].playerId == 203500
        assert mapped[1].firstName == 'Steven'

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
