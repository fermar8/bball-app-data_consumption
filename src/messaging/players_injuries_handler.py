"""
Messaging/Handler layer for the data-consumption-players-injuries Lambda.
"""
import json
import logging
import os
from typing import Any, Dict

from jsonschema import ValidationError, validate

from src.database.database import DynamoDBConnection
from src.repository.players_injuries_repository import PlayersInjuriesRepository
from src.service.players_injuries_service import PlayersInjuriesService

logger = logging.getLogger(__name__)

RAW_SCHEMA_PATH = os.path.join(os.path.dirname(__file__), 'schemas', 'injury-report-raw-schema.json')
with open(RAW_SCHEMA_PATH, 'r') as _f:
    RAW_INJURY_REPORT_SCHEMA = json.load(_f)


class PlayersInjuriesHandler:
    """Handler for the players_injuries data consumption Lambda."""

    def __init__(self, service: PlayersInjuriesService = None):
        self.service = service

    def handle(self, event: Dict[str, Any] = None) -> Dict[str, Any]:
        try:
            if self.service is None:
                repository = PlayersInjuriesRepository()
                self.service = PlayersInjuriesService(repository)

            raw_document = self.service.fetch_latest_injury_report_document()
            validate(instance=raw_document, schema=RAW_INJURY_REPORT_SCHEMA)
            result = self.service.consume_injuries_from_document(raw_document)

            logger.info("consume_injuries completed. written_injuries=%d", result['written_injuries'])
            return {
                'statusCode': 200,
                'body': json.dumps({
                    'message': 'Injuries consumed and persisted successfully',
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
            logger.error("Error processing request: %s", exc, exc_info=True)
            return {
                'statusCode': 500,
                'body': json.dumps({'error': 'Internal server error'}),
            }


def lambda_handler(event, context):
    """Lambda entry point for data-consumption-players-injuries."""
    logger.setLevel(logging.INFO)
    logger.info("Processing Lambda request")
    logger.info("Event: %s", json.dumps(event))

    try:
        DynamoDBConnection.initialize()

        handler = PlayersInjuriesHandler()
        response = handler.handle(event=event)

        logger.info("Response status: %s", response.get('statusCode'))
        return response

    except Exception as exc:
        logger.error("Unhandled error in lambda_handler: %s", exc, exc_info=True)
        return {
            'statusCode': 500,
            'body': json.dumps({'error': 'Internal server error'}),
        }
