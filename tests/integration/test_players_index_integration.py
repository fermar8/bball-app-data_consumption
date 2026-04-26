"""
Integration tests for players_index DynamoDB and S3 operations using moto.
"""
import json
import os

import boto3
import pytest
from moto import mock_aws

from src.database.database import DynamoDBConnection
from src.messaging.players_index_handler import lambda_handler
from src.repository.players_index_repository import PlayersIndexRepository
from src.service.players_index_service import PlayersIndexService


@pytest.fixture
def aws_credentials():
    os.environ['AWS_ACCESS_KEY_ID'] = 'testing'
    os.environ['AWS_SECRET_ACCESS_KEY'] = 'testing'
    os.environ['AWS_SECURITY_TOKEN'] = 'testing'
    os.environ['AWS_SESSION_TOKEN'] = 'testing'
    os.environ['AWS_DEFAULT_REGION'] = 'eu-west-3'


@pytest.fixture
def dynamodb_table(aws_credentials):
    with mock_aws():
        os.environ['DYNAMODB_TABLE_NAME'] = 'bball-app-data-consumption-players-index-nonlive'

        dynamodb = boto3.resource('dynamodb', region_name='eu-west-3')
        table = dynamodb.create_table(
            TableName='bball-app-data-consumption-players-index-nonlive',
            KeySchema=[
                {'AttributeName': 'playerId', 'KeyType': 'HASH'},
            ],
            AttributeDefinitions=[
                {'AttributeName': 'playerId', 'AttributeType': 'N'},
            ],
            BillingMode='PAY_PER_REQUEST',
        )

        DynamoDBConnection._table = None
        DynamoDBConnection._table_name = None

        yield table

        del os.environ['DYNAMODB_TABLE_NAME']
        DynamoDBConnection._table = None
        DynamoDBConnection._table_name = None


@pytest.fixture
def s3_bucket_with_player_index(aws_credentials, dynamodb_table):
    with mock_aws():
        os.environ['S3_BUCKET_NAME'] = 'bball-app-nba-data'

        s3 = boto3.client('s3', region_name='eu-west-3')
        s3.create_bucket(
            Bucket='bball-app-nba-data',
            CreateBucketConfiguration={'LocationConstraint': 'eu-west-3'},
        )

        player_index_payload = {
            'source': 'nba_api',
            'endpoint': 'player_index',
            'fetched_at_utc': '2026-04-26T13:00:00Z',
            'aws_account_id': '590183661886',
            'params': {},
            'payload': {
                'resource': 'playerindex',
                'parameters': {},
                'resultSets': [
                    {
                        'name': 'PlayerIndex',
                        'headers': [
                            'PERSON_ID',
                            'PLAYER_FIRST_NAME',
                            'PLAYER_LAST_NAME',
                            'POSITION',
                            'JERSEY_NUMBER',
                            'HEIGHT',
                            'COUNTRY',
                            'ROSTER_STATUS',
                        ],
                        'rowSet': [
                            [2544, 'LeBron', 'James', 'F', 23, '6-9', 'USA', 1],
                            [201939, 'Stephen', 'Curry', 'G', 30, '6-2', 'USA', 1],
                        ],
                    }
                ],
            },
        }

        s3.put_object(
            Bucket='bball-app-nba-data',
            Key='raw/player_index/2026/04/26/13/20260426T130000Z_abc12345.json',
            Body=json.dumps(player_index_payload),
        )

        yield s3

        del os.environ['S3_BUCKET_NAME']
        DynamoDBConnection._table = None
        DynamoDBConnection._table_name = None


class TestPlayersIndexIntegration:
    @mock_aws
    def test_lambda_handler_success(self, aws_credentials, s3_bucket_with_player_index):
        response = lambda_handler({}, None)

        assert response['statusCode'] == 200
        body = json.loads(response['body'])
        assert body['message'] == 'Players consumed and persisted successfully'
        assert body['flattened_players'] == 2
        assert body['mapped_players'] == 2
        assert body['written_players'] == 2

    @mock_aws
    def test_players_persisted_to_dynamodb(self, aws_credentials, s3_bucket_with_player_index):
        lambda_handler({}, None)

        DynamoDBConnection.initialize()
        repository = PlayersIndexRepository()

        lebron = repository.get_by_id(2544)
        assert lebron is not None
        assert lebron.firstName == 'LeBron'
        assert lebron.lastName == 'James'
        assert lebron.jerseyNumber == 23

        stephen = repository.get_by_id(201939)
        assert stephen is not None
        assert stephen.firstName == 'Stephen'
        assert stephen.lastName == 'Curry'
        assert stephen.jerseyNumber == 30

    @mock_aws
    def test_hash_prevents_duplicate_writes(self, aws_credentials, s3_bucket_with_player_index):
        lambda_handler({}, None)
        response1 = lambda_handler({}, None)

        body1 = json.loads(response1['body'])
        assert body1['written_players'] == 2

        response2 = lambda_handler({}, None)
        body2 = json.loads(response2['body'])
        assert body2['written_players'] == 0
