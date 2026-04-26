"""
Repository layer for DynamoDB operations on the games table.
"""
import json
import hashlib
from decimal import Decimal
from typing import Any, Dict, List, Optional

from botocore.exceptions import ClientError

from src.database.database import DynamoDBConnection
from src.model.models import NbaGame


class GamesRepository:
    """Data access layer for NbaGame using DynamoDB."""

    def __init__(self):
        self.table = DynamoDBConnection.get_table()

    @staticmethod
    def build_data_hash(item: Dict[str, Any]) -> str:
        """Create a deterministic hash from persisted game attributes."""
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
    def _item_to_model(item: Dict[str, Any]) -> NbaGame:
        def to_int(key: str) -> Optional[int]:
            if key not in item:
                return None
            return int(item[key])

        return NbaGame(
            gameId=item['gameId'],
            leagueKey=item.get('leagueKey', 'NBA'),
            gameDateEst=item.get('gameDateEst', ''),
            gameDateTimeEst=item.get('gameDateTimeEst', ''),
            gameStatus=int(item.get('gameStatus', 0)),
            gameStatusText=item.get('gameStatusText', ''),
            homeTeamId=int(item.get('homeTeamId', 0)),
            homeTeamName=item.get('homeTeamName', ''),
            homeTeamTricode=item.get('homeTeamTricode', ''),
            awayTeamId=int(item.get('awayTeamId', 0)),
            awayTeamName=item.get('awayTeamName', ''),
            awayTeamTricode=item.get('awayTeamTricode', ''),
            homeTeamWins=to_int('homeTeamWins'),
            homeTeamLosses=to_int('homeTeamLosses'),
            homeTeamScore=to_int('homeTeamScore'),
            awayTeamWins=to_int('awayTeamWins'),
            awayTeamLosses=to_int('awayTeamLosses'),
            awayTeamScore=to_int('awayTeamScore'),
            arenaName=item.get('arenaName'),
            arenaCity=item.get('arenaCity'),
            dataHash=item.get('dataHash'),
        )

    def upsert_changed(self, game: NbaGame) -> bool:
        """Insert/update the game only when the payload hash changed."""
        item = game.to_dict()
        data_hash = self.build_data_hash(item)
        item['dataHash'] = data_hash
        game.dataHash = data_hash

        try:
            self.table.put_item(
                Item=self._to_dynamodb_item(item),
                ConditionExpression='attribute_not_exists(gameId) OR dataHash <> :hash',
                ExpressionAttributeValues={':hash': data_hash},
            )
            return True
        except ClientError as exc:
            if exc.response.get('Error', {}).get('Code') == 'ConditionalCheckFailedException':
                return False
            raise

    def upsert_changed_batch(self, games: List[NbaGame]) -> int:
        """Upsert only changed items and return the number of writes."""
        writes = 0
        for game in games:
            if self.upsert_changed(game):
                writes += 1
        return writes

    def get_by_id(self, game_id: str) -> Optional[NbaGame]:
        response = self.table.get_item(Key={'gameId': game_id})
        if 'Item' not in response:
            return None
        return self._item_to_model(response['Item'])

    def get_all(self) -> List[NbaGame]:
        response = self.table.scan()
        return [self._item_to_model(item) for item in response.get('Items', [])]
