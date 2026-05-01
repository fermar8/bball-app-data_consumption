"""
Messaging/Handler layer for the data-consumption-games Lambda.
"""
import json
import logging
import os
from typing import Any, Dict

from jsonschema import ValidationError, validate

from src.database.database import DynamoDBConnection
from src.repository.games_repository import GamesRepository
from src.service.games_service import GamesService

logger = logging.getLogger(__name__)

RAW_SCHEMA_PATH = os.path.join(os.path.dirname(__file__), 'schemas', 'games-raw-schema.json')
with open(RAW_SCHEMA_PATH, 'r') as _f:
    RAW_GAMES_SCHEMA = json.load(_f)


class GamesHandler:
    """Handler for the games data consumption Lambda."""

    def __init__(self, service: GamesService = None):
        self.service = service

    def handle(self, event: Dict[str, Any] = None) -> Dict[str, Any]:
        try:
            if self.service is None:
                repository = GamesRepository()
                self.service = GamesService(repository)

            runtime_input: Dict[str, Any] = {}
            if isinstance(event, dict):
                if isinstance(event.get('input'), dict):
                    runtime_input = event['input']
                else:
                    runtime_input = event

            allowed_keys = {
                'write_all_season_games',
                'from_date_utc',
                'to_date_utc',
                'replay_until_default_horizon',
                'include_final_games',
                'refresh_days',
            }

            if isinstance(event, dict) and isinstance(event.get('input'), dict):
                unknown_keys = sorted([k for k in runtime_input.keys() if k not in allowed_keys])
                if unknown_keys:
                    raise ValueError(f"Unknown input option(s): {', '.join(unknown_keys)}")

            input_options = {k: v for k, v in runtime_input.items() if k in allowed_keys}

            raw_document = self.service.fetch_latest_schedule_document()
            validate(instance=raw_document, schema=RAW_GAMES_SCHEMA)
            result = self.service.consume_games_from_document(raw_document, input_options=input_options)

            logger.info("consume_games completed. written_games=%d", result['written_games'])
            return {
                'statusCode': 200,
                'body': json.dumps({
                    'message': 'Games consumed and persisted successfully',
                    **result,
                }),
            }

        except ValidationError as exc:
            logger.error("Schema validation error: %s", exc.message)
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
            logger.error("Error processing request: %s", exc, exc_info=True)
            return {
                'statusCode': 500,
                'body': json.dumps({'error': 'Internal server error'}),
            }


def lambda_handler(event, context):
    """Lambda entry point for data-consumption-games."""
    logger.setLevel(logging.INFO)
    logger.info("Processing Lambda request")
    logger.info("Event: %s", json.dumps(event))

    try:
        DynamoDBConnection.initialize()

        handler = GamesHandler()
        response = handler.handle(event=event)

        logger.info("Response status: %s", response.get('statusCode'))
        return response

    except Exception as exc:
        logger.error("Unhandled error in lambda_handler: %s", exc, exc_info=True)
        return {
            'statusCode': 500,
            'body': json.dumps({'error': 'Internal server error'}),
        }
