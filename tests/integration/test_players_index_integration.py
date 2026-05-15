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
            'fetched_at_utc': '2026-03-10T11:40:09.005839+00:00',
            'aws_account_id': '590183661886',
            'schema_version': 'v1',
            'ingestion_id': '10beff59-807e-42da-b685-5f1ad63ec9e2',
            'params': {'active': 1},
            'payload': {
                'resource': 'playerindex',
                'parameters': {
                    'LeagueID': '00',
                    'Season': '2025-26',
                    'Historical': 0,
                    'TeamID': 0,
                    'Country': None,
                    'College': None,
                    'DraftYear': None,
                    'DraftPick': None,
                    'PlayerPosition': '',
                    'Height': None,
                    'Weight': None,
                    'Active': 1,
                    'AllStar': 0,
                },
                'resultSets': [
                    {
                        'name': 'PlayerIndex',
                        'headers': [
                            'PERSON_ID',
                            'PLAYER_LAST_NAME',
                            'PLAYER_FIRST_NAME',
                            'PLAYER_SLUG',
                            'TEAM_ID',
                            'TEAM_SLUG',
                            'IS_DEFUNCT',
                            'TEAM_CITY',
                            'TEAM_NAME',
                            'TEAM_ABBREVIATION',
                            'JERSEY_NUMBER',
                            'POSITION',
                            'HEIGHT',
                            'WEIGHT',
                            'COLLEGE',
                            'COUNTRY',
                            'DRAFT_YEAR',
                            'DRAFT_ROUND',
                            'DRAFT_NUMBER',
                            'ROSTER_STATUS',
                            'FROM_YEAR',
                            'TO_YEAR',
                            'PTS',
                            'REB',
                            'AST',
                            'STATS_TIMEFRAME',
                        ],
                        'rowSet': [
                            [
                                1630173, 'Achiuwa', 'Precious', 'precious-achiuwa',
                                1610612758, 'kings', 0, 'Sacramento', 'Kings', 'SAC',
                                '9', 'F', '6-8', '243', 'Memphis', 'Nigeria',
                                2020, 1, 20, 1, '2020', '2025', 8.7, 6.1, 1.3, 'Season',
                            ],
                            [
                                203500, 'Adams', 'Steven', 'steven-adams',
                                1610612745, 'rockets', 0, 'Houston', 'Rockets', 'HOU',
                                '12', 'C', '6-11', '265', 'Pittsburgh', 'New Zealand',
                                2013, 1, 12, 1, '2013', '2025', 5.8, 8.6, 1.5, 'Season',
                            ],
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
    def test_lambda_handler_success(self, aws_credentials, s3_bucket_with_player_index):
        response = lambda_handler({}, None)

        assert response['statusCode'] == 200
        body = json.loads(response['body'])
        assert body['message'] == 'Players consumed and persisted successfully'
        assert body['flattened_players'] == 2
        assert body['mapped_players'] == 2
        assert body['written_players'] == 2

    def test_players_persisted_to_dynamodb(self, aws_credentials, s3_bucket_with_player_index):
        lambda_handler({}, None)

        DynamoDBConnection.initialize()
        repository = PlayersIndexRepository()

        achiuwa = repository.get_by_id(1630173)
        assert achiuwa is not None
        assert achiuwa.firstName == 'Precious'
        assert achiuwa.lastName == 'Achiuwa'
        assert achiuwa.displayName == 'Precious Achiuwa'
        assert achiuwa.teamId == 1610612758
        assert achiuwa.teamName == 'Kings'
        assert achiuwa.teamAbbreviation == 'SAC'
        assert achiuwa.jerseyNumber == '9'

        adams = repository.get_by_id(203500)
        assert adams is not None
        assert adams.firstName == 'Steven'
        assert adams.lastName == 'Adams'
        assert adams.teamAbbreviation == 'HOU'
        assert adams.jerseyNumber == '12'

    def test_hash_prevents_duplicate_writes(self, aws_credentials, s3_bucket_with_player_index):
        response1 = lambda_handler({}, None)

        body1 = json.loads(response1['body'])
        assert body1['written_players'] == 2

        response2 = lambda_handler({}, None)
        body2 = json.loads(response2['body'])
        assert body2['written_players'] == 0
