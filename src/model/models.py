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
