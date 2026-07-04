"""
Repository layer for DynamoDB operations on the players_injuries table.
"""
import json
import hashlib
from decimal import Decimal
from typing import Any, Dict, List, Optional

from boto3.dynamodb.conditions import Key
from botocore.exceptions import ClientError

from src.database.database import DynamoDBConnection
from src.model.models import NbaPlayerInjury


class PlayersInjuriesRepository:
    """Data access layer for NbaPlayerInjury using DynamoDB."""

    def __init__(self):
        self.table = DynamoDBConnection.get_table()

    @staticmethod
    def build_data_hash(item: Dict[str, Any]) -> str:
        """Create a deterministic hash from persisted injury attributes."""
        canonical = {k: item[k] for k in sorted(item.keys()) if k != 'dataHash'}
        payload = json.dumps(canonical, sort_keys=True, separators=(',', ':'))
        return hashlib.sha256(payload.encode('utf-8')).hexdigest()

    @staticmethod
    def _to_dynamodb_item(item: Dict[str, Any]) -> Dict[str, Any]:
        dynamo_item: Dict[str, Any] = {}
        for key, value in item.items():
            if isinstance(value, int):
                dynamo_item[key] = Decimal(str(value))
            else:
                dynamo_item[key] = value
        return dynamo_item

    @staticmethod
    def _item_to_model(item: Dict[str, Any]) -> NbaPlayerInjury:
        return NbaPlayerInjury(
            playerId=int(item['playerId']),
            injuryKey=item.get('injuryKey', ''),
            playerName=item.get('playerName', ''),
            teamAbbr=item.get('teamAbbr', ''),
            status=item.get('status', ''),
            availability=item.get('availability', ''),
            reasonType=item.get('reasonType', ''),
            reason=item.get('reason', ''),
            reportDate=item.get('reportDate', ''),
            fetchedAt=item.get('fetchedAt', ''),
            updatedAt=item.get('updatedAt'),
            dataHash=item.get('dataHash'),
        )

    def upsert_changed(self, injury: NbaPlayerInjury) -> bool:
        """Insert/update the injury only when the payload hash changed."""
        item = injury.to_dict()
        data_hash = self.build_data_hash(item)
        item['dataHash'] = data_hash
        injury.dataHash = data_hash

        try:
            self.table.put_item(
                Item=self._to_dynamodb_item(item),
                ConditionExpression='attribute_not_exists(playerId) OR dataHash <> :hash',
                ExpressionAttributeValues={':hash': data_hash},
            )
            return True
        except ClientError as exc:
            if exc.response.get('Error', {}).get('Code') == 'ConditionalCheckFailedException':
                return False
            raise

    def upsert_changed_batch(self, injuries: List[NbaPlayerInjury]) -> int:
        writes = 0
        for injury in injuries:
            if self.upsert_changed(injury):
                writes += 1
        return writes

    def get_by_key(self, player_id: int, injury_key: str) -> Optional[NbaPlayerInjury]:
        response = self.table.get_item(Key={'playerId': player_id, 'injuryKey': injury_key})
        if 'Item' not in response:
            return None
        return self._item_to_model(response['Item'])

    def get_by_player(self, player_id: int) -> List[NbaPlayerInjury]:
        response = self.table.query(KeyConditionExpression=Key('playerId').eq(player_id))
        return [self._item_to_model(item) for item in response.get('Items', [])]

    def get_by_team(self, team_abbr: str) -> List[NbaPlayerInjury]:
        response = self.table.query(
            IndexName='byTeamReportDate',
            KeyConditionExpression=Key('teamAbbr').eq(team_abbr),
        )
        return [self._item_to_model(item) for item in response.get('Items', [])]

    def get_all(self) -> List[NbaPlayerInjury]:
        response = self.table.scan()
        return [self._item_to_model(item) for item in response.get('Items', [])]
