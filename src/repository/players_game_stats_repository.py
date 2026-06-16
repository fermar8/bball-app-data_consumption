"""
Repository layer for DynamoDB operations on the player game stats table.
"""
import hashlib
import json
from decimal import Decimal
from typing import Any, Dict, List, Optional

from botocore.exceptions import ClientError

from src.database.database import DynamoDBConnection
from src.model.models import NbaPlayerGameLog


class PlayersGameStatsRepository:
    """Data access layer for NbaPlayerGameLog using DynamoDB."""

    def __init__(self):
        self.table = DynamoDBConnection.get_table()

    @staticmethod
    def build_data_hash(item: Dict[str, Any]) -> str:
        """Create a deterministic hash from persisted player game log attributes."""
        canonical = {k: item[k] for k in sorted(item.keys()) if k != 'dataHash'}
        payload = json.dumps(canonical, sort_keys=True, separators=(',', ':'))
        return hashlib.sha256(payload.encode('utf-8')).hexdigest()

    @staticmethod
    def _to_dynamodb_item(item: Dict[str, Any]) -> Dict[str, Any]:
        dynamo_item: Dict[str, Any] = {}
        for key, value in item.items():
            if isinstance(value, bool):
                dynamo_item[key] = value
            elif isinstance(value, (int, float)):
                dynamo_item[key] = Decimal(str(value))
            else:
                dynamo_item[key] = value
        return dynamo_item

    @staticmethod
    def _item_to_model(item: Dict[str, Any]) -> NbaPlayerGameLog:
        def to_int(key: str) -> Optional[int]:
            if key not in item:
                return None
            return int(item[key])

        def to_float(key: str) -> Optional[float]:
            if key not in item:
                return None
            return float(item[key])

        return NbaPlayerGameLog(
            playerId=int(item['playerId']),
            playerName=item.get('playerName', ''),
            nickname=item.get('nickname', ''),
            teamId=int(item.get('teamId', 0)),
            teamAbbreviation=item.get('teamAbbreviation', ''),
            teamName=item.get('teamName', ''),
            gameId=item.get('gameId', ''),
            gameDate=item.get('gameDate', ''),
            matchup=item.get('matchup', ''),
            winLoss=item.get('winLoss', ''),
            minutes=item.get('minutes', ''),
            minutesDecimal=to_float('minutesDecimal'),
            seasonYear=item.get('seasonYear', ''),
            fgm=int(item.get('fgm', 0)),
            fga=int(item.get('fga', 0)),
            fgPct=to_float('fgPct'),
            fg3m=int(item.get('fg3m', 0)),
            fg3a=int(item.get('fg3a', 0)),
            fg3Pct=to_float('fg3Pct'),
            ftm=int(item.get('ftm', 0)),
            fta=int(item.get('fta', 0)),
            ftPct=to_float('ftPct'),
            oreb=int(item.get('oreb', 0)),
            dreb=int(item.get('dreb', 0)),
            reb=int(item.get('reb', 0)),
            ast=int(item.get('ast', 0)),
            tov=int(item.get('tov', 0)),
            stl=int(item.get('stl', 0)),
            blk=int(item.get('blk', 0)),
            blka=int(item.get('blka', 0)),
            pf=int(item.get('pf', 0)),
            pfd=int(item.get('pfd', 0)),
            pts=int(item.get('pts', 0)),
            plusMinus=to_float('plusMinus'),
            nbaFantasyPts=to_float('nbaFantasyPts'),
            dd2=int(item.get('dd2', 0)),
            td3=int(item.get('td3', 0)),
            wnbaFantasyPts=to_float('wnbaFantasyPts'),
            availableFlag=int(item.get('availableFlag', 0)),
            minSec=item.get('minSec', ''),
            teamCount=int(item.get('teamCount', 0)),
            dataHash=item.get('dataHash'),
        )

    def upsert_changed(self, player_game_log: NbaPlayerGameLog) -> bool:
        """Insert/update the game log only when the payload hash changed."""
        item = player_game_log.to_dict()
        data_hash = self.build_data_hash(item)
        item['dataHash'] = data_hash
        player_game_log.dataHash = data_hash

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

    def upsert_changed_batch(self, player_game_logs: List[NbaPlayerGameLog]) -> int:
        """Upsert only changed items and return the number of writes."""
        writes = 0
        for player_game_log in player_game_logs:
            if self.upsert_changed(player_game_log):
                writes += 1
        return writes

    def get_by_player_and_game(self, player_id: int, game_date_game_id: str) -> Optional[NbaPlayerGameLog]:
        response = self.table.get_item(Key={'playerId': player_id, 'gameDateGameId': game_date_game_id})
        if 'Item' not in response:
            return None
        return self._item_to_model(response['Item'])

    def get_all(self) -> List[NbaPlayerGameLog]:
        response = self.table.scan()
        return [self._item_to_model(item) for item in response.get('Items', [])]
