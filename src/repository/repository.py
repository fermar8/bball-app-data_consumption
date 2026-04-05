"""
Repository layer for DynamoDB operations on the teams-static table.
"""
from typing import List
from decimal import Decimal

from src.database.database import DynamoDBConnection
from src.model.models import NbaTeam


class TeamRepository:
    """Data access layer for NbaTeam using DynamoDB."""

    def __init__(self):
        """Initialize repository with DynamoDB table."""
        self.table = DynamoDBConnection.get_table()

    def upsert(self, team: NbaTeam) -> NbaTeam:
        """
        Insert or update an NBA team record in DynamoDB.

        Args:
            team: NbaTeam object to persist.

        Returns:
            The persisted NbaTeam object.
        """
        item = {
            'teamId': Decimal(str(team.teamId)),
            'fullName': team.fullName,
            'abbreviation': team.abbreviation,
            'nickname': team.nickname,
            'city': team.city,
            'state': team.state,
            'yearFounded': Decimal(str(team.yearFounded)),
        }
        self.table.put_item(Item=item)
        return team

    def upsert_batch(self, teams: List[NbaTeam]) -> int:
        """
        Batch-insert or update a list of NBA team records.

        Uses DynamoDB batch_writer for efficiency.

        Args:
            teams: List of NbaTeam objects to persist.

        Returns:
            Number of teams persisted.
        """
        with self.table.batch_writer() as batch:
            for team in teams:
                batch.put_item(Item={
                    'teamId': Decimal(str(team.teamId)),
                    'fullName': team.fullName,
                    'abbreviation': team.abbreviation,
                    'nickname': team.nickname,
                    'city': team.city,
                    'state': team.state,
                    'yearFounded': Decimal(str(team.yearFounded)),
                })
        return len(teams)

    def get_by_id(self, team_id: int) -> NbaTeam | None:
        """
        Retrieve a team by its teamId.

        Args:
            team_id: Integer team identifier.

        Returns:
            NbaTeam if found, None otherwise.
        """
        response = self.table.get_item(Key={'teamId': Decimal(str(team_id))})
        if 'Item' not in response:
            return None
        return self._item_to_model(response['Item'])

    def get_all(self) -> List[NbaTeam]:
        """
        Scan all teams from the table.

        Returns:
            List of NbaTeam objects.
        """
        response = self.table.scan()
        return [self._item_to_model(item) for item in response.get('Items', [])]

    @staticmethod
    def _item_to_model(item: dict) -> NbaTeam:
        return NbaTeam(
            teamId=int(item['teamId']),
            fullName=item.get('fullName', ''),
            abbreviation=item.get('abbreviation', ''),
            nickname=item.get('nickname', ''),
            city=item.get('city', ''),
            state=item.get('state', ''),
            yearFounded=int(item.get('yearFounded', 0)),
        )
