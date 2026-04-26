"""
Messaging/Handler layer for the data-consumption-players-index Lambda.
"""
import json
import logging
import os
from typing import Any, Dict

from jsonschema import ValidationError, validate

from src.database.database import DynamoDBConnection
from src.repository.players_index_repository import PlayersIndexRepository
from src.service.players_index_service import PlayersIndexService

logger = logging.getLogger(__name__)

RAW_SCHEMA_PATH = os.path.join(os.path.dirname(__file__), 'schemas', 'player-index-raw-schema.json')
with open(RAW_SCHEMA_PATH, 'r') as _f:
    RAW_PLAYER_INDEX_SCHEMA = json.load(_f)


class PlayersIndexHandler:
    """Handler for the players_index data consumption Lambda."""

    def __init__(self, service: PlayersIndexService = None):
        self.service = service

    def handle(self, event: Dict[str, Any] = None) -> Dict[str, Any]:
        try:
            if self.service is None:
                repository = PlayersIndexRepository()
                self.service = PlayersIndexService(repository)

            raw_document = self.service.fetch_latest_player_index_document()
            validate(instance=raw_document, schema=RAW_PLAYER_INDEX_SCHEMA)
            result = self.service.consume_players_from_document(raw_document)

            logger.info("consume_players completed. written_players=%d", result['written_players'])
            return {
                'statusCode': 200,
                'body': json.dumps({
                    'message': 'Players consumed and persisted successfully',
                    **result,
                }),
            }

        except ValidationError as exc:
            logger.warning("Schema validation error: %s", exc.message)
            return {
                'statusCode': 400,
                'body': json.dumps({'error': f'Validation error: {exc.message}'}),
            }
        except (FileNotFoundError, ValueError) as exc:
            logger.error("Data error: %s", exc)
            return {
                'statusCode': 422,
                'body': json.dumps({'error': str(exc)}),
            }
        except Exception as exc:
            logger.exception("Unexpected error: %s", exc)
            return {
                'statusCode': 500,
                'body': json.dumps({'error': 'Internal server error'}),
            }


def lambda_handler(event, context):
    """Lambda entry point for data-consumption-players-index."""
    logger.setLevel(logging.INFO)
    logger.info("Processing Lambda request")
    logger.info("Event: %s", json.dumps(event))

    try:
        DynamoDBConnection.initialize()

        handler = PlayersIndexHandler()
        response = handler.handle(event=event)

        logger.info("Response status: %s", response.get('statusCode'))
        return response

    except Exception as exc:
        logger.error("Unhandled error in lambda_handler: %s", exc, exc_info=True)
        return {
            'statusCode': 500,
            'body': json.dumps({'error': 'Internal server error'}),
        }

