"""
Repository layer for DynamoDB operations on the players_index table.
"""
import json
import hashlib
from decimal import Decimal
from typing import Any, Dict, List, Optional

from botocore.exceptions import ClientError

from src.database.database import DynamoDBConnection
from src.model.models import NbaPlayer


class PlayersIndexRepository:
    """Data access layer for NbaPlayer using DynamoDB."""

    def __init__(self):
        self.table = DynamoDBConnection.get_table()

    @staticmethod
    def build_data_hash(item: Dict[str, Any]) -> str:
        """Create a deterministic hash from persisted player attributes."""
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
    def _item_to_model(item: Dict[str, Any]) -> NbaPlayer:
        def to_int(key: str) -> Optional[int]:
            if key not in item:
                return None
            return int(item[key])

        return NbaPlayer(
            playerId=int(item['playerId']),
            firstName=item.get('firstName', ''),
            lastName=item.get('lastName', ''),
            position=item.get('position', ''),
            jerseyNumber=to_int('jerseyNumber'),
            height=item.get('height', ''),
            country=item.get('country', ''),
            rosterStatus=int(item.get('rosterStatus', 0)),
            dataHash=item.get('dataHash'),
        )

    def upsert_changed(self, player: NbaPlayer) -> bool:
        """Insert/update the player only when the payload hash changed."""
        item = player.to_dict()
        data_hash = self.build_data_hash(item)
        item['dataHash'] = data_hash
        player.dataHash = data_hash

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

    def upsert_changed_batch(self, players: List[NbaPlayer]) -> int:
        """Upsert only changed items and return the number of writes."""
        writes = 0
        for player in players:
            if self.upsert_changed(player):
                writes += 1
        return writes

    def get_by_id(self, player_id: int) -> Optional[NbaPlayer]:
        response = self.table.get_item(Key={'playerId': player_id})
        if 'Item' not in response:
            return None
        return self._item_to_model(response['Item'])

    def get_all(self) -> List[NbaPlayer]:
        response = self.table.scan()
        return [self._item_to_model(item) for item in response.get('Items', [])]
