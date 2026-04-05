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
    city: str = ""
    state: str = ""
    yearFounded: int = 0

    def to_dict(self) -> dict:
        """Convert model to dictionary."""
        return {
            'teamId': self.teamId,
            'fullName': self.fullName,
            'abbreviation': self.abbreviation,
            'nickname': self.nickname,
            'city': self.city,
            'state': self.state,
            'yearFounded': self.yearFounded,
        }

    @classmethod
    def from_raw(cls, raw: dict) -> 'NbaTeam':
        """
        Map a raw teams_static dict from nba_api / S3 to an NbaTeam instance.

        Args:
            raw: Raw dict with keys id, full_name, abbreviation, nickname,
                 city, state, year_founded.

        Returns:
            NbaTeam instance.
        """
        return cls(
            teamId=int(raw['id']),
            fullName=raw.get('full_name', ''),
            abbreviation=raw.get('abbreviation', ''),
            nickname=raw.get('nickname', ''),
            city=raw.get('city', ''),
            state=raw.get('state', ''),
            yearFounded=int(raw.get('year_founded', 0)),
        )
