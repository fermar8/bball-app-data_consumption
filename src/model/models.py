"""
Database models for the NBA teams data consumption application.
"""
from dataclasses import dataclass
from typing import Optional


@dataclass
class NbaTeam:
    """
    NBA team model matching the teams-static DynamoDB table structure.
    """
    teamId: int = 0
    fullName: str = ""
    abbreviation: str = ""
    nickname: str = ""

    def to_dict(self) -> dict:
        """Convert model to dictionary."""
        return {
            'teamId': self.teamId,
            'fullName': self.fullName,
            'abbreviation': self.abbreviation,
            'nickname': self.nickname,
        }

    @classmethod
    def from_raw(cls, raw: dict) -> 'NbaTeam':
        """
        Map a raw teams_static dict from nba_api / S3 to an NbaTeam instance.

        Args:
            raw: Raw dict with keys id, full_name, abbreviation, nickname.

        Returns:
            NbaTeam instance.
        """
        return cls(
            teamId=int(raw['id']),
            fullName=raw['full_name'],
            abbreviation=raw['abbreviation'],
            nickname=raw['nickname'],
        )


@dataclass
class NbaGame:
    """NBA game model matching the games DynamoDB table structure."""

    gameId: str = ""
    leagueKey: str = "NBA"
    gameDateEst: str = ""
    gameDateTimeEst: str = ""
    gameStatus: int = 0
    gameStatusText: str = ""
    homeTeamId: int = 0
    homeTeamName: str = ""
    homeTeamTricode: str = ""
    awayTeamId: int = 0
    awayTeamName: str = ""
    awayTeamTricode: str = ""
    homeTeamWins: Optional[int] = None
    homeTeamLosses: Optional[int] = None
    homeTeamScore: Optional[int] = None
    awayTeamWins: Optional[int] = None
    awayTeamLosses: Optional[int] = None
    awayTeamScore: Optional[int] = None
    arenaName: Optional[str] = None
    arenaCity: Optional[str] = None
    dataHash: Optional[str] = None

    def to_dict(self) -> dict:
        """Convert model to a sparse dictionary for persistence."""
        item = {
            'gameId': self.gameId,
            'leagueKey': self.leagueKey,
            'gameDateEst': self.gameDateEst,
            'gameDateTimeEst': self.gameDateTimeEst,
            'gameStatus': self.gameStatus,
            'gameStatusText': self.gameStatusText.strip(),
            'homeTeamId': self.homeTeamId,
            'homeTeamName': self.homeTeamName,
            'homeTeamTricode': self.homeTeamTricode,
            'awayTeamId': self.awayTeamId,
            'awayTeamName': self.awayTeamName,
            'awayTeamTricode': self.awayTeamTricode,
        }
        if self.homeTeamWins is not None:
            item['homeTeamWins'] = self.homeTeamWins
        if self.homeTeamLosses is not None:
            item['homeTeamLosses'] = self.homeTeamLosses
        if self.homeTeamScore is not None:
            item['homeTeamScore'] = self.homeTeamScore
        if self.awayTeamWins is not None:
            item['awayTeamWins'] = self.awayTeamWins
        if self.awayTeamLosses is not None:
            item['awayTeamLosses'] = self.awayTeamLosses
        if self.awayTeamScore is not None:
            item['awayTeamScore'] = self.awayTeamScore
        if self.arenaName:
            item['arenaName'] = self.arenaName
        if self.arenaCity:
            item['arenaCity'] = self.arenaCity
        if self.dataHash:
            item['dataHash'] = self.dataHash
        return item

    @classmethod
    def from_raw(cls, raw: dict) -> 'NbaGame':
        """Map a raw schedule_league_v2 game dict to an NbaGame instance."""
        return cls(
            gameId=str(raw['gameId']),
            leagueKey='NBA',
            gameDateEst=raw['gameDateEst'],
            gameDateTimeEst=raw['gameDateTimeEst'],
            gameStatus=int(raw['gameStatus']),
            gameStatusText=str(raw['gameStatusText']).strip(),
            homeTeamId=int(raw['homeTeam']['teamId']),
            homeTeamName=raw['homeTeam']['teamName'],
            homeTeamTricode=raw['homeTeam']['teamTricode'],
            awayTeamId=int(raw['awayTeam']['teamId']),
            awayTeamName=raw['awayTeam']['teamName'],
            awayTeamTricode=raw['awayTeam']['teamTricode'],
            homeTeamWins=_to_optional_int(raw.get('homeTeam', {}).get('wins')),
            homeTeamLosses=_to_optional_int(raw.get('homeTeam', {}).get('losses')),
            homeTeamScore=_to_optional_int(raw.get('homeTeam', {}).get('score')),
            awayTeamWins=_to_optional_int(raw.get('awayTeam', {}).get('wins')),
            awayTeamLosses=_to_optional_int(raw.get('awayTeam', {}).get('losses')),
            awayTeamScore=_to_optional_int(raw.get('awayTeam', {}).get('score')),
            arenaName=raw.get('arenaName') or None,
            arenaCity=raw.get('arenaCity') or None,
        )


def _to_optional_int(value) -> Optional[int]:
    if value is None:
        return None
    return int(value)


@dataclass
class NbaPlayer:
    """NBA player model matching the players_index DynamoDB table structure.

    Field set follows the contract documented in
    bball-app-nba_api_client/docs/DATABASE_SCHEMA.md (Table 3: nba_players).
    Stats (PTS/REB/AST) and historical metadata (DRAFT_*, FROM_YEAR, TO_YEAR,
    WEIGHT, COLLEGE) are intentionally excluded; they belong to future tables
    (`nba_player_stats`) or are deferred. `injuryStatus` will be added when the
    injuries pipeline lands and will be denormalized here for fast roster reads.
    """

    playerId: int = 0
    firstName: str = ""
    lastName: str = ""
    displayName: str = ""
    teamId: int = 0
    teamName: str = ""
    teamAbbreviation: str = ""
    position: str = ""
    jerseyNumber: Optional[str] = None
    height: str = ""
    country: str = ""
    rosterStatus: int = 0
    dataHash: Optional[str] = None

    def to_dict(self) -> dict:
        """Convert model to a sparse dictionary for persistence."""
        item = {
            'playerId': self.playerId,
            'firstName': self.firstName,
            'lastName': self.lastName,
            'displayName': self.displayName,
            'teamId': self.teamId,
            'teamName': self.teamName,
            'teamAbbreviation': self.teamAbbreviation,
            'position': self.position,
            'height': self.height,
            'country': self.country,
            'rosterStatus': self.rosterStatus,
        }
        if self.jerseyNumber is not None and self.jerseyNumber != "":
            item['jerseyNumber'] = self.jerseyNumber
        if self.dataHash:
            item['dataHash'] = self.dataHash
        return item

    @classmethod
    def from_raw(cls, raw: dict) -> 'NbaPlayer':
        """
        Map a raw player_index dict from nba_api / S3 to an NbaPlayer instance.

        Args:
            raw: Raw dict with keys PERSON_ID, PLAYER_FIRST_NAME, PLAYER_LAST_NAME, etc.

        Returns:
            NbaPlayer instance.
        """
        first_name = str(raw.get('PLAYER_FIRST_NAME', '')).strip()
        last_name = raw['PLAYER_LAST_NAME'].strip()
        full_name = f"{first_name} {last_name}".strip()
        jersey_raw = raw.get('JERSEY_NUMBER')
        jersey = str(jersey_raw).strip() if jersey_raw not in (None, '') else None
        return cls(
            playerId=int(raw['PERSON_ID']),
            firstName=first_name,
            lastName=last_name,
            displayName=full_name,
            teamId=int(raw.get('TEAM_ID') or 0),
            teamName=str(raw.get('TEAM_NAME') or '').strip(),
            teamAbbreviation=str(raw.get('TEAM_ABBREVIATION') or '').strip(),
            position=str(raw.get('POSITION', '') or '').strip(),
            jerseyNumber=jersey,
            height=str(raw.get('HEIGHT', '') or '').strip(),
            country=str(raw.get('COUNTRY', '') or '').strip(),
            rosterStatus=int(raw.get('ROSTER_STATUS') or 0),
        )
