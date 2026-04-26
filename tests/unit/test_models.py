"""
Unit tests for the NbaTeam model.
"""
import pytest

from src.model.models import NbaTeam
from src.model.models import NbaGame
from src.model.models import NbaPlayer


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
        assert game.leagueKey == 'NBA'
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
        assert data['leagueKey'] == 'NBA'
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


class TestNbaPlayerModel:
    """Unit tests for NbaPlayer dataclass."""

    def test_from_raw_maps_required_fields(self):
        raw = {
            'PERSON_ID': 2544,
            'PLAYER_FIRST_NAME': 'LeBron',
            'PLAYER_LAST_NAME': 'James',
            'POSITION': 'F',
            'JERSEY_NUMBER': 23,
            'HEIGHT': '6-9',
            'COUNTRY': 'USA',
            'ROSTER_STATUS': 1,
        }
        player = NbaPlayer.from_raw(raw)
        assert player.playerId == 2544
        assert player.firstName == 'LeBron'
        assert player.lastName == 'James'
        assert player.position == 'F'
        assert player.jerseyNumber == 23
        assert player.height == '6-9'
        assert player.country == 'USA'
        assert player.rosterStatus == 1

    def test_from_raw_coerces_person_id_to_int(self):
        raw = {
            'PERSON_ID': '2544',
            'PLAYER_FIRST_NAME': 'LeBron',
            'PLAYER_LAST_NAME': 'James',
        }
        player = NbaPlayer.from_raw(raw)
        assert isinstance(player.playerId, int)
        assert player.playerId == 2544

    def test_from_raw_handles_missing_optional_fields(self):
        raw = {
            'PERSON_ID': 1234,
            'PLAYER_FIRST_NAME': 'Rookie',
            'PLAYER_LAST_NAME': 'Player',
        }
        player = NbaPlayer.from_raw(raw)
        assert player.position == ''
        assert player.jerseyNumber is None
        assert player.height == ''
        assert player.country == ''
        assert player.rosterStatus == 0

    def test_from_raw_strips_whitespace_in_text_fields(self):
        raw = {
            'PERSON_ID': 1,
            'PLAYER_FIRST_NAME': '  LeBron  ',
            'PLAYER_LAST_NAME': '  James  ',
            'POSITION': '  F  ',
            'HEIGHT': '  6-9  ',
            'COUNTRY': '  USA  ',
        }
        player = NbaPlayer.from_raw(raw)
        assert player.firstName == 'LeBron'
        assert player.lastName == 'James'
        assert player.position == 'F'
        assert player.height == '6-9'
        assert player.country == 'USA'

    def test_from_raw_raises_on_missing_required_fields(self):
        with pytest.raises(KeyError):
            NbaPlayer.from_raw({'PLAYER_FIRST_NAME': 'No ID'})

    def test_to_dict_includes_required_fields(self):
        player = NbaPlayer(
            playerId=2544,
            firstName='LeBron',
            lastName='James',
            position='F',
            jerseyNumber=23,
            height='6-9',
            country='USA',
            rosterStatus=1,
        )
        data = player.to_dict()
        assert data['playerId'] == 2544
        assert data['firstName'] == 'LeBron'
        assert data['lastName'] == 'James'
        assert data['position'] == 'F'
        assert data['jerseyNumber'] == 23
        assert data['height'] == '6-9'
        assert data['country'] == 'USA'
        assert data['rosterStatus'] == 1

    def test_to_dict_omits_missing_optional_fields(self):
        player = NbaPlayer(
            playerId=2544,
            firstName='LeBron',
            lastName='James',
        )
        data = player.to_dict()
        assert 'jerseyNumber' not in data
        assert 'dataHash' not in data
        assert data['playerId'] == 2544

    def test_default_values(self):
        player = NbaPlayer()
        assert player.playerId == 0
        assert player.firstName == ''
        assert player.lastName == ''
        assert player.position == ''
        assert player.jerseyNumber is None
        assert player.height == ''
        assert player.country == ''
        assert player.rosterStatus == 0
        assert player.dataHash is None

    def test_roundtrip_from_raw_to_dict(self):
        raw = {
            'PERSON_ID': 201939,
            'PLAYER_FIRST_NAME': 'Stephen',
            'PLAYER_LAST_NAME': 'Curry',
            'POSITION': 'G',
            'JERSEY_NUMBER': 30,
            'HEIGHT': '6-2',
            'COUNTRY': 'USA',
            'ROSTER_STATUS': 1,
        }
        data = NbaPlayer.from_raw(raw).to_dict()
        assert data['playerId'] == 201939
        assert data['firstName'] == 'Stephen'
        assert data['lastName'] == 'Curry'
        assert data['jerseyNumber'] == 30
        assert data['rosterStatus'] == 1
