"""
Messaging/Handler layer for the data-consumption-teams-static Lambda.

Reads the latest teams_static raw payload from S3, maps it to the NbaTeam
structure, and persists every team record to the DynamoDB table that matches
the current environment (nonlive / live).
"""
import json
import logging
import os
from typing import Any, Dict

from jsonschema import validate, ValidationError

from src.database.database import DynamoDBConnection
from src.repository.teams_static_repository import TeamsStaticRepository
from src.service.teams_static_service import TeamsStaticService

logger = logging.getLogger(__name__)

RAW_SCHEMA_PATH = os.path.join(os.path.dirname(__file__), 'schemas', 'teams-static-raw-schema.json')
with open(RAW_SCHEMA_PATH, 'r') as _f:
    RAW_TEAMS_SCHEMA = json.load(_f)


class TeamsStaticHandler:
    """Handler for the teams-static data consumption Lambda."""

    def __init__(self, service: TeamsStaticService = None):
        """
        Initialize the handler.

        Args:
            service: TeamsStaticService instance (for dependency injection).
        """
        self.service = service

    def handle(self) -> Dict[str, Any]:
        """
        Fetch the latest teams-static raw document from S3, validate it,
        and persist every team record to DynamoDB.

        Returns:
            Response dict with statusCode and body.
        """
        try:
            if self.service is None:
                repository = TeamsStaticRepository()
                self.service = TeamsStaticService(repository)

            raw_document = self.service.fetch_latest_teams_document()
            validate(instance=raw_document, schema=RAW_TEAMS_SCHEMA)
            count = self.service.consume_teams_from_document(raw_document)
            logger.info("consume_teams_static completed. teams_persisted=%d", count)
            return {
                'statusCode': 200,
                'body': json.dumps({
                    'message': 'Teams consumed and persisted successfully',
                    'teams_persisted': count,
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
    """
    Lambda entry point for data-consumption-teams-static.

    Args:
        event: Lambda event.
        context: Lambda context object.

    Returns:
        dict: Response with statusCode and body.
    """
    logger.setLevel(logging.INFO)
    logger.info("Processing Lambda request")
    logger.info("Event: %s", json.dumps(event))

    try:
        DynamoDBConnection.initialize()

        handler = TeamsStaticHandler()
        response = handler.handle()

        logger.info("Response status: %s", response.get('statusCode'))
        return response

    except Exception as exc:
        logger.error("Unhandled error in lambda_handler: %s", exc, exc_info=True)
        return {
            'statusCode': 500,
            'body': json.dumps({'error': 'Internal server error'}),
        }
