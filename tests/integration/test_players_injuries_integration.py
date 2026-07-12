"""
Integration tests for players_injuries DynamoDB and S3 operations using moto.
"""
import json
import os

import boto3
import pytest
from moto import mock_aws

from src.database.database import DynamoDBConnection
from src.messaging.players_injuries_handler import lambda_handler
from src.repository.players_injuries_repository import PlayersInjuriesRepository


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
        os.environ['DYNAMODB_TABLE_NAME'] = 'bball-app-data-consumption-players-injuries-nonlive'

        dynamodb = boto3.resource('dynamodb', region_name='eu-west-3')
        table = dynamodb.create_table(
            TableName='bball-app-data-consumption-players-injuries-nonlive',
            KeySchema=[
                {'AttributeName': 'playerId', 'KeyType': 'HASH'},
                {'AttributeName': 'injuryKey', 'KeyType': 'RANGE'},
            ],
            AttributeDefinitions=[
                {'AttributeName': 'playerId', 'AttributeType': 'N'},
                {'AttributeName': 'injuryKey', 'AttributeType': 'S'},
                {'AttributeName': 'teamAbbr', 'AttributeType': 'S'},
                {'AttributeName': 'reportDate', 'AttributeType': 'S'},
            ],
            GlobalSecondaryIndexes=[
                {
                    'IndexName': 'byTeamReportDate',
                    'KeySchema': [
                        {'AttributeName': 'teamAbbr', 'KeyType': 'HASH'},
                        {'AttributeName': 'reportDate', 'KeyType': 'RANGE'},
                    ],
                    'Projection': {'ProjectionType': 'ALL'},
                }
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
def s3_bucket_with_injury_report(aws_credentials, dynamodb_table):
    with mock_aws():
        os.environ['S3_BUCKET_NAME'] = 'bball-app-nba-data'

        s3 = boto3.client('s3', region_name='eu-west-3')
        s3.create_bucket(
            Bucket='bball-app-nba-data',
            CreateBucketConfiguration={'LocationConstraint': 'eu-west-3'},
        )

        payload = {
            'source': 'nba_com',
            'endpoint': 'injury_report',
            'fetched_at_utc': '2026-05-28T13:00:00+00:00',
            'aws_account_id': '590183661886',
            'schema_version': 'v1',
            'ingestion_id': 'abc-123',
            'params': {},
            'payload': {
                'source': 'nba_com',
                'raw_entries_count': 3,
                'count': 3,
                'updated_at': '2026-05-28T12:59:00+00:00',
                'injuries': [
                    {
                        'player_id': 2544,
                        'player_name': 'LeBron James',
                        'team_abbr': 'LAL',
                        'status': 'out',
                        'availability': 'no',
                        'reason_type': 'injury',
                        'reason': 'Left knee soreness',
                        'report_date': '05/27/2026',
                    },
                    {
                        'player_id': 203954,
                        'player_name': 'Joel Embiid',
                        'team_abbr': 'PHI',
                        'status': 'questionable',
                        'availability': 'doubtful',
                        'reason_type': 'injury',
                        'reason': 'Left knee',
                        'report_date': '05/27/2026',
                    },
                    {
                        'player_id': None,
                        'player_name': 'Ghost Player',
                        'team_abbr': 'LA Clippers',
                        'status': 'available',
                        'availability': 'yes',
                        'reason_type': 'unknown',
                        'reason': '',
                        'report_date': '05/27/2026',
                    },
                ],
            },
        }

        s3.put_object(
            Bucket='bball-app-nba-data',
            Key='raw/injury_report/2026/05/28/13/20260528T130000Z_abc12345.json',
            Body=json.dumps(payload),
        )

        yield s3

        del os.environ['S3_BUCKET_NAME']
        DynamoDBConnection._table = None
        DynamoDBConnection._table_name = None


class TestPlayersInjuriesIntegration:
    def test_lambda_handler_success(self, aws_credentials, s3_bucket_with_injury_report):
        response = lambda_handler({}, None)

        assert response['statusCode'] == 200
        body = json.loads(response['body'])
        assert body['message'] == 'Injuries consumed and persisted successfully'
        assert body['flattened_injuries'] == 3
        assert body['mapped_injuries'] == 2
        assert body['written_injuries'] == 2

    def test_injuries_persisted_to_dynamodb(self, aws_credentials, s3_bucket_with_injury_report):
        lambda_handler({}, None)

        DynamoDBConnection.initialize()
        repository = PlayersInjuriesRepository()

        lebron_injuries = repository.get_by_player(2544)
        assert len(lebron_injuries) == 1
        assert lebron_injuries[0].playerName == 'LeBron James'
        assert lebron_injuries[0].teamAbbr == 'LAL'
        assert lebron_injuries[0].status == 'out'
        assert lebron_injuries[0].reportDate == '2026-05-27'
        assert lebron_injuries[0].fetchedAt == '2026-05-28T13:00:00+00:00'

        embiid_injuries = repository.get_by_player(203954)
        assert len(embiid_injuries) == 1
        assert embiid_injuries[0].status == 'questionable'

    def test_gsi_query_by_team(self, aws_credentials, s3_bucket_with_injury_report):
        lambda_handler({}, None)

        DynamoDBConnection.initialize()
        repository = PlayersInjuriesRepository()

        lakers = repository.get_by_team('LAL')
        assert len(lakers) == 1
        assert lakers[0].playerId == 2544

    def test_hash_prevents_duplicate_writes(self, aws_credentials, s3_bucket_with_injury_report):
        response1 = lambda_handler({}, None)
        body1 = json.loads(response1['body'])
        assert body1['written_injuries'] == 2

        response2 = lambda_handler({}, None)
        body2 = json.loads(response2['body'])
        assert body2['written_injuries'] == 0
