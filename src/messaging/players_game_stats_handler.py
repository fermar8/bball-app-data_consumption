"""
Messaging/Handler layer for the player game stats Lambda.
"""
import json
import logging
import os
from typing import Any, Dict

from jsonschema import ValidationError, validate

from src.database.database import DynamoDBConnection
from src.repository.players_game_stats_repository import PlayersGameStatsRepository
from src.service.players_game_stats_service import PlayersGameStatsService

logger = logging.getLogger(__name__)

RAW_SCHEMA_PATH = os.path.join(os.path.dirname(__file__), 'schemas', 'player-game-logs-raw-schema.json')
with open(RAW_SCHEMA_PATH, 'r') as _f:
    RAW_PLAYER_GAME_LOGS_SCHEMA = json.load(_f)


class PlayersGameStatsHandler:
    """Handler for the player game stats data consumption Lambda."""

    def __init__(self, service: PlayersGameStatsService = None):
        self.service = service

    def handle(self) -> Dict[str, Any]:
        try:
            if self.service is None:
                repository = PlayersGameStatsRepository()
                self.service = PlayersGameStatsService(repository)

            raw_document = self.service.fetch_latest_player_game_logs_document()
            validate(instance=raw_document, schema=RAW_PLAYER_GAME_LOGS_SCHEMA)
            result = self.service.consume_player_game_logs_from_document(raw_document)
            logger.info("consume_player_game_logs completed. written_rows=%d", result['written_player_game_logs'])
            return {
                'statusCode': 200,
                'body': json.dumps({
                    'message': 'Player game logs consumed and persisted successfully',
                    **result,
                }),
            }

        except ValidationError as exc:
            field_path = '.'.join(str(p) for p in exc.absolute_path) if exc.absolute_path else 'root'
            logger.error("Schema validation error at '%s': %s", field_path, exc.message)
            return {
                'statusCode': 400,
                'body': json.dumps({'error': f'Validation error at {field_path}: {exc.message}'}),
            }
        except (FileNotFoundError, ValueError) as exc:
            logger.error("Data error: %s", exc)
            return {
                'statusCode': 422,
                'body': json.dumps({'error': str(exc)}),
            }
        except Exception as exc:
            logger.error("Error processing request: %s", exc, exc_info=True)
            return {
                'statusCode': 500,
                'body': json.dumps({'error': 'Internal server error'}),
            }


def lambda_handler(event, context):
    """Lambda entry point for player game stats ingestion."""
    logger.setLevel(logging.INFO)
    logger.info("Processing Lambda request")
    logger.info("Event: %s", json.dumps(event))

    try:
        DynamoDBConnection.initialize()

        handler = PlayersGameStatsHandler()
        response = handler.handle()

        logger.info("Response status: %s", response.get('statusCode'))
        return response

    except Exception as exc:
        logger.error("Unhandled error in lambda_handler: %s", exc, exc_info=True)
        return {
            'statusCode': 500,
            'body': json.dumps({'error': 'Internal server error'}),
        }
