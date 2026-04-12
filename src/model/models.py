"""
Database models for the NBA teams data consumption application.
"""
from dataclasses import dataclass


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
