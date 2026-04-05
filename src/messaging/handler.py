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
from src.repository.repository import TeamRepository
from src.service.service import TeamsConsumptionService

logger = logging.getLogger(__name__)

SCHEMA_PATH = os.path.join(os.path.dirname(__file__), 'schemas', 'lambda-event-schema.json')
with open(SCHEMA_PATH, 'r') as _f:
    EVENT_SCHEMA = json.load(_f)


class Handler:
    """Handler for the teams-static data consumption Lambda."""

    def __init__(self, service: TeamsConsumptionService = None):
        """
        Initialize the handler.

        Args:
            service: TeamsConsumptionService instance (for dependency injection).
        """
        self.service = service

    def handle(self, event: Dict[str, Any]) -> Dict[str, Any]:
        """
        Handle a Lambda event.

        Expected event shape::

            {"action": "consume_teams_static"}

        Args:
            event: Lambda event dict.

        Returns:
            Response dict with statusCode and body.
        """
        try:
            validate(instance=event, schema=EVENT_SCHEMA)

            if self.service is None:
                repository = TeamRepository()
                self.service = TeamsConsumptionService(repository)

            action = event.get('action')

            if action == 'consume_teams_static':
                count = self.service.consume_teams()
                logger.info("consume_teams_static completed. teams_persisted=%d", count)
                return {
                    'statusCode': 200,
                    'body': json.dumps({
                        'message': 'Teams consumed and persisted successfully',
                        'teams_persisted': count,
                    }),
                }

            return {
                'statusCode': 400,
                'body': json.dumps({'error': f'Unknown action: {action}'}),
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
        event: Lambda event. Expected shape: ``{"action": "consume_teams_static"}``
        context: Lambda context object.

    Returns:
        dict: Response with statusCode and body.
    """
    logger.setLevel(logging.INFO)
    logger.info("Processing Lambda request")
    logger.info("Event: %s", json.dumps(event))

    try:
        DynamoDBConnection.initialize()

        handler = Handler()
        response = handler.handle(event)

        logger.info("Response status: %s", response.get('statusCode'))
        return response

    except Exception as exc:
        logger.error("Unhandled error in lambda_handler: %s", exc, exc_info=True)
        return {
            'statusCode': 500,
            'body': json.dumps({'error': 'Internal server error'}),
        }
