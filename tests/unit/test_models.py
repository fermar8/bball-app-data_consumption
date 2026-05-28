"""
Unit tests for the NbaTeam model.
"""
import pytest

from src.model.models import NbaTeam
from src.model.models import NbaGame
from src.model.models import NbaPlayer


PLAYER_INDEX_SAMPLE = {
    'PERSON_ID': 1630173,
    'PLAYER_LAST_NAME': 'Achiuwa',
    'PLAYER_FIRST_NAME': 'Precious',
    'PLAYER_SLUG': 'precious-achiuwa',
    'TEAM_ID': 1610612758,
    'TEAM_SLUG': 'kings',
    'IS_DEFUNCT': 0,
    'TEAM_CITY': 'Sacramento',
    'TEAM_NAME': 'Kings',
    'TEAM_ABBREVIATION': 'SAC',
    'JERSEY_NUMBER': '9',
    'POSITION': 'F',
    'HEIGHT': '6-8',
    'WEIGHT': '243',
    'COLLEGE': 'Memphis',
    'COUNTRY': 'Nigeria',
    'DRAFT_YEAR': 2020,
    'DRAFT_ROUND': 1,
    'DRAFT_NUMBER': 20,
    'ROSTER_STATUS': 1,
    'FROM_YEAR': '2020',
    'TO_YEAR': '2025',
    'PTS': 8.7,
    'REB': 6.1,
    'AST': 1.3,
    'STATS_TIMEFRAME': 'Season',
}


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
        raw = dict(PLAYER_INDEX_SAMPLE)
        player = NbaPlayer.from_raw(raw)
        assert player.playerId == 1630173
        assert player.firstName == 'Precious'
        assert player.lastName == 'Achiuwa'
        assert player.displayName == 'Precious Achiuwa'
        assert player.teamId == 1610612758
        assert player.teamName == 'Kings'
        assert player.teamAbbreviation == 'SAC'
        assert player.position == 'F'
        assert player.jerseyNumber == '9'
        assert player.height == '6-8'
        assert player.country == 'Nigeria'
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
        assert player.teamId == 0
        assert player.teamName == ''
        assert player.teamAbbreviation == ''
        assert player.displayName == 'Rookie Player'

    def test_from_raw_strips_whitespace_in_text_fields(self):
        raw = dict(PLAYER_INDEX_SAMPLE)
        raw['PERSON_ID'] = 1
        raw['PLAYER_FIRST_NAME'] = '  Precious  '
        raw['PLAYER_LAST_NAME'] = '  Achiuwa  '
        raw['POSITION'] = '  F  '
        raw['HEIGHT'] = '  6-8  '
        raw['COUNTRY'] = '  Nigeria  '
        player = NbaPlayer.from_raw(raw)
        assert player.firstName == 'Precious'
        assert player.lastName == 'Achiuwa'
        assert player.position == 'F'
        assert player.height == '6-8'
        assert player.country == 'Nigeria'

    def test_from_raw_raises_on_missing_required_fields(self):
        with pytest.raises(KeyError):
            NbaPlayer.from_raw({'PLAYER_FIRST_NAME': 'No ID'})

    def test_to_dict_includes_required_fields(self):
        player = NbaPlayer(
            playerId=2544,
            firstName='LeBron',
            lastName='James',
            displayName='LeBron James',
            teamId=1610612747,
            teamName='Lakers',
            teamAbbreviation='LAL',
            position='F',
            jerseyNumber='23',
            height='6-9',
            country='USA',
            rosterStatus=1,
        )
        data = player.to_dict()
        assert data['playerId'] == 2544
        assert data['firstName'] == 'LeBron'
        assert data['lastName'] == 'James'
        assert data['displayName'] == 'LeBron James'
        assert data['teamId'] == 1610612747
        assert data['teamName'] == 'Lakers'
        assert data['teamAbbreviation'] == 'LAL'
        assert data['position'] == 'F'
        assert data['jerseyNumber'] == '23'
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
            'JERSEY_NUMBER': '30',
            'HEIGHT': '6-2',
            'COUNTRY': 'USA',
            'ROSTER_STATUS': 1,
        }
        data = NbaPlayer.from_raw(raw).to_dict()
        assert data['playerId'] == 201939
        assert data['firstName'] == 'Stephen'
        assert data['lastName'] == 'Curry'
        assert data['jerseyNumber'] == '30'
        assert data['rosterStatus'] == 1


from src.model.models import NbaPlayerInjury


INJURY_SAMPLE = {
    'player_id': 2544,
    'player_name': 'LeBron James',
    'team_abbr': 'LAL',
    'status': 'questionable',
    'availability': 'doubtful',
    'reason_type': 'injury',
    'reason': 'Left knee soreness',
    'report_date': '05/27/2026',
}


class TestNbaPlayerInjuryModel:
    """Unit tests for NbaPlayerInjury dataclass."""

    def test_from_raw_maps_required_fields(self):
        injury = NbaPlayerInjury.from_raw(
            INJURY_SAMPLE,
            fetched_at='2026-05-28T13:00:00+00:00',
        )
        assert injury.playerId == 2544
        assert injury.playerName == 'LeBron James'
        assert injury.teamAbbr == 'LAL'
        assert injury.status == 'questionable'
        assert injury.availability == 'doubtful'
        assert injury.reasonType == 'injury'
        assert injury.reason == 'Left knee soreness'

    def test_from_raw_normalizes_us_date_to_iso(self):
        injury = NbaPlayerInjury.from_raw(
            INJURY_SAMPLE,
            fetched_at='2026-05-28T13:00:00+00:00',
        )
        assert injury.reportDate == '2026-05-27'

    def test_from_raw_passes_through_iso_date(self):
        raw = {**INJURY_SAMPLE, 'report_date': '2026-05-27'}
        injury = NbaPlayerInjury.from_raw(raw, fetched_at='2026-05-28T13:00:00+00:00')
        assert injury.reportDate == '2026-05-27'

    def test_from_raw_builds_composite_injury_key(self):
        injury = NbaPlayerInjury.from_raw(
            INJURY_SAMPLE,
            fetched_at='2026-05-28T13:00:00+00:00',
        )
        assert injury.injuryKey == '2026-05-27#2026-05-28T13:00:00+00:00'

    def test_from_raw_stores_updated_at_when_provided(self):
        injury = NbaPlayerInjury.from_raw(
            INJURY_SAMPLE,
            fetched_at='2026-05-28T13:00:00+00:00',
            updated_at='2026-05-28T12:59:00+00:00',
        )
        assert injury.updatedAt == '2026-05-28T12:59:00+00:00'

    def test_from_raw_raises_when_player_id_missing(self):
        raw = {**INJURY_SAMPLE, 'player_id': None}
        with pytest.raises(ValueError):
            NbaPlayerInjury.from_raw(raw, fetched_at='2026-05-28T13:00:00+00:00')

    def test_to_dict_contains_required_fields(self):
        injury = NbaPlayerInjury.from_raw(
            INJURY_SAMPLE,
            fetched_at='2026-05-28T13:00:00+00:00',
        )
        data = injury.to_dict()
        assert data['playerId'] == 2544
        assert data['injuryKey'] == '2026-05-27#2026-05-28T13:00:00+00:00'
        assert data['teamAbbr'] == 'LAL'
        assert data['status'] == 'questionable'
        assert data['reportDate'] == '2026-05-27'
        assert data['fetchedAt'] == '2026-05-28T13:00:00+00:00'

    def test_to_dict_omits_missing_optional_fields(self):
        injury = NbaPlayerInjury(
            playerId=2544,
            injuryKey='2026-05-27#2026-05-28T13:00:00+00:00',
            playerName='LeBron James',
            teamAbbr='LAL',
            status='out',
            availability='no',
            reasonType='injury',
            reason='knee',
            reportDate='2026-05-27',
            fetchedAt='2026-05-28T13:00:00+00:00',
        )
        data = injury.to_dict()
        assert 'updatedAt' not in data
        assert 'dataHash' not in data

    def test_default_values(self):
        injury = NbaPlayerInjury()
        assert injury.playerId == 0
        assert injury.injuryKey == ''
        assert injury.dataHash is None
        assert injury.updatedAt is None
