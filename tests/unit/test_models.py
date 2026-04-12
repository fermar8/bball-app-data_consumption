"""
Unit tests for the NbaTeam model.
"""
import pytest

from src.model.models import NbaTeam
from src.model.models import NbaGame


class TestNbaTeamModel:
    """Unit tests for NbaTeam dataclass."""

    def test_to_dict_contains_all_fields(self):
        """to_dict should return all expected keys."""
        team = NbaTeam(
            teamId=1610612737,
            fullName='Atlanta Hawks',
            abbreviation='ATL',
            nickname='Hawks',
        )
        d = team.to_dict()
        assert d['teamId'] == 1610612737
        assert d['fullName'] == 'Atlanta Hawks'
        assert d['abbreviation'] == 'ATL'
        assert d['nickname'] == 'Hawks'
        assert len(d.keys()) == 4

    def test_from_raw_maps_all_fields(self):
        """from_raw should correctly map all raw nba_api keys."""
        raw = {
            'id': 1610612737,
            'full_name': 'Atlanta Hawks',
            'abbreviation': 'ATL',
            'nickname': 'Hawks',
        }
        team = NbaTeam.from_raw(raw)
        assert team.teamId == 1610612737
        assert team.fullName == 'Atlanta Hawks'
        assert team.abbreviation == 'ATL'
        assert team.nickname == 'Hawks'

    def test_from_raw_coerces_id_to_int(self):
        """from_raw should convert string id to int."""
        raw = {
            'id': '1610612737',
            'full_name': 'Hawks',
            'abbreviation': 'HWK',
            'nickname': 'Hawks',
        }
        team = NbaTeam.from_raw(raw)
        assert isinstance(team.teamId, int)
        assert team.teamId == 1610612737

    def test_from_raw_raises_on_missing_id(self):
        """from_raw should raise KeyError when id is missing."""
        with pytest.raises(KeyError):
            NbaTeam.from_raw({'full_name': 'No ID Team'})

    def test_from_raw_raises_on_missing_required_text_fields(self):
        """from_raw should raise KeyError when required raw fields are missing."""
        with pytest.raises(KeyError):
            NbaTeam.from_raw({'id': 1, 'full_name': 'No Abbreviation'})

    def test_default_values(self):
        """NbaTeam should have sensible defaults."""
        team = NbaTeam()
        assert team.teamId == 0
        assert team.fullName == ''
        assert team.abbreviation == ''
        assert team.nickname == ''

    def test_roundtrip_from_raw_to_dict(self):
        """from_raw followed by to_dict should preserve all data."""
        raw = {
            'id': 1610612738,
            'full_name': 'Boston Celtics',
            'abbreviation': 'BOS',
            'nickname': 'Celtics',
        }
        d = NbaTeam.from_raw(raw).to_dict()
        assert d['teamId'] == 1610612738
        assert d['fullName'] == 'Boston Celtics'
        assert d['abbreviation'] == 'BOS'
        assert d['nickname'] == 'Celtics'


class TestNbaGameModel:
    """Unit tests for NbaGame dataclass."""

    def test_from_raw_maps_required_fields(self):
        raw = {
            'gameId': '0022500001',
            'gameStatus': 1,
            'gameStatusText': 'Scheduled',
            'gameDateEst': '2025-10-22T00:00:00Z',
            'gameDateTimeEst': '2025-10-22T19:30:00Z',
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
            'arenaName': 'Crypto.com Arena',
            'arenaCity': 'Los Angeles',
        }
        game = NbaGame.from_raw(raw)
        assert game.gameId == '0022500001'
        assert game.gameStatus == 1
        assert game.homeTeamId == 1610612747
        assert game.awayTeamTricode == 'BOS'

    def test_to_dict_omits_missing_optional_fields(self):
        game = NbaGame(
            gameId='0022500001',
            gameDateEst='2025-10-22T00:00:00Z',
            gameDateTimeEst='2025-10-22T19:30:00Z',
            gameStatus=1,
            gameStatusText='Scheduled',
            homeTeamId=1610612747,
            homeTeamName='Lakers',
            homeTeamTricode='LAL',
            awayTeamId=1610612738,
            awayTeamName='Celtics',
            awayTeamTricode='BOS',
        )
        data = game.to_dict()
        assert 'homeTeamScore' not in data
        assert 'awayTeamWins' not in data
        assert data['gameId'] == '0022500001'

    def test_to_dict_trims_status_text(self):
        game = NbaGame(
            gameId='0022500002',
            gameDateEst='2025-10-22T00:00:00Z',
            gameDateTimeEst='2025-10-22T20:30:00Z',
            gameStatus=3,
            gameStatusText='Final   ',
            homeTeamId=1610612747,
            homeTeamName='Lakers',
            homeTeamTricode='LAL',
            awayTeamId=1610612738,
            awayTeamName='Celtics',
            awayTeamTricode='BOS',
        )
        data = game.to_dict()
        assert data['gameStatusText'] == 'Final'
