"""
Unit tests for the NbaTeam model.
"""
import pytest

from src.model.models import NbaTeam


class TestNbaTeamModel:
    """Unit tests for NbaTeam dataclass."""

    def test_to_dict_contains_all_fields(self):
        """to_dict should return all expected keys."""
        team = NbaTeam(
            teamId=1610612737,
            fullName='Atlanta Hawks',
            abbreviation='ATL',
            nickname='Hawks',
            city='Atlanta',
            state='Georgia',
            yearFounded=1949,
        )
        d = team.to_dict()
        assert d['teamId'] == 1610612737
        assert d['fullName'] == 'Atlanta Hawks'
        assert d['abbreviation'] == 'ATL'
        assert d['nickname'] == 'Hawks'
        assert d['city'] == 'Atlanta'
        assert d['state'] == 'Georgia'
        assert d['yearFounded'] == 1949

    def test_from_raw_maps_all_fields(self):
        """from_raw should correctly map all raw nba_api keys."""
        raw = {
            'id': 1610612737,
            'full_name': 'Atlanta Hawks',
            'abbreviation': 'ATL',
            'nickname': 'Hawks',
            'city': 'Atlanta',
            'state': 'Georgia',
            'year_founded': 1949,
        }
        team = NbaTeam.from_raw(raw)
        assert team.teamId == 1610612737
        assert team.fullName == 'Atlanta Hawks'
        assert team.abbreviation == 'ATL'
        assert team.nickname == 'Hawks'
        assert team.city == 'Atlanta'
        assert team.state == 'Georgia'
        assert team.yearFounded == 1949

    def test_from_raw_handles_missing_optional_fields(self):
        """from_raw should handle missing optional fields gracefully."""
        raw = {'id': 1610612737}
        team = NbaTeam.from_raw(raw)
        assert team.teamId == 1610612737
        assert team.fullName == ''
        assert team.abbreviation == ''
        assert team.nickname == ''
        assert team.city == ''
        assert team.state == ''
        assert team.yearFounded == 0

    def test_from_raw_coerces_id_to_int(self):
        """from_raw should convert string id to int."""
        raw = {'id': '1610612737', 'full_name': 'Hawks', 'year_founded': '1949'}
        team = NbaTeam.from_raw(raw)
        assert isinstance(team.teamId, int)
        assert team.teamId == 1610612737
        assert isinstance(team.yearFounded, int)
        assert team.yearFounded == 1949

    def test_from_raw_raises_on_missing_id(self):
        """from_raw should raise KeyError when id is missing."""
        with pytest.raises(KeyError):
            NbaTeam.from_raw({'full_name': 'No ID Team'})

    def test_default_values(self):
        """NbaTeam should have sensible defaults."""
        team = NbaTeam()
        assert team.teamId == 0
        assert team.fullName == ''
        assert team.yearFounded == 0

    def test_roundtrip_from_raw_to_dict(self):
        """from_raw followed by to_dict should preserve all data."""
        raw = {
            'id': 1610612738,
            'full_name': 'Boston Celtics',
            'abbreviation': 'BOS',
            'nickname': 'Celtics',
            'city': 'Boston',
            'state': 'Massachusetts',
            'year_founded': 1946,
        }
        d = NbaTeam.from_raw(raw).to_dict()
        assert d['teamId'] == 1610612738
        assert d['fullName'] == 'Boston Celtics'
        assert d['yearFounded'] == 1946
