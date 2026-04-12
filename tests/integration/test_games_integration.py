"""
Integration tests for games DynamoDB and S3 operations using moto.
"""
import json
import os

import boto3
import pytest
from moto import mock_aws

from src.database.database import DynamoDBConnection
from src.messaging.games_handler import lambda_handler
from src.repository.games_repository import GamesRepository
from src.service.games_service import GamesService


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
        os.environ['DYNAMODB_TABLE_NAME'] = 'bball-app-data-consumption-games-nonlive'

        dynamodb = boto3.resource('dynamodb', region_name='eu-west-3')
        table = dynamodb.create_table(
            TableName='bball-app-data-consumption-games-nonlive',
            KeySchema=[
                {'AttributeName': 'gameId', 'KeyType': 'HASH'},
            ],
            AttributeDefinitions=[
                {'AttributeName': 'gameId', 'AttributeType': 'S'},
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
def s3_bucket_with_schedule(aws_credentials, dynamodb_table):
    with mock_aws():
        os.environ['S3_BUCKET_NAME'] = 'bball-app-nba-data'

        s3 = boto3.client('s3', region_name='eu-west-3')
        s3.create_bucket(
            Bucket='bball-app-nba-data',
            CreateBucketConfiguration={'LocationConstraint': 'eu-west-3'},
        )

        older_payload = {
            'source': 'nba_api',
            'endpoint': 'schedule_league_v2',
            'fetched_at_utc': '2026-03-07T02:00:00.000000+00:00Z',
            'aws_account_id': '590183661886',
            'params': {},
            'payload': {
                'leagueSchedule': {
                    'gameDates': [
                        {
                            'gameDate': '10/03/2025 00:00:00',
                            'games': [
                                {
                                    'gameId': '0022500001',
                                    'gameStatus': 1,
                                    'gameStatusText': 'Scheduled',
                                    'gameDateEst': '2025-10-03T00:00:00Z',
                                    'gameDateTimeEst': '2025-10-03T22:00:00Z',
                                    'gameLabel': 'Regular Season',
                                    'arenaName': 'Acrisure Arena',
                                    'arenaCity': 'Palm Desert',
                                    'homeTeam': {
                                        'teamId': 1610612747,
                                        'teamName': 'Lakers',
                                        'teamTricode': 'LAL',
                                        'wins': 0,
                                        'losses': 0,
                                        'score': None,
                                    },
                                    'awayTeam': {
                                        'teamId': 1610612756,
                                        'teamName': 'Suns',
                                        'teamTricode': 'PHX',
                                        'wins': 0,
                                        'losses': 0,
                                        'score': None,
                                    },
                                }
                            ],
                        }
                    ]
                }
            },
        }

        newer_payload = {
            'source': 'nba_api',
            'endpoint': 'schedule_league_v2',
            'fetched_at_utc': '2026-03-07T03:00:02.441991+00:00Z',
            'aws_account_id': '590183661886',
            'params': {},
            'payload': {
                'leagueSchedule': {
                    'gameDates': [
                        {
                            'gameDate': '10/03/2025 00:00:00',
                            'games': [
                                {
                                    'gameId': '0022500001',
                                    'gameStatus': 3,
                                    'gameStatusText': 'Final',
                                    'gameDateEst': '2025-10-03T00:00:00Z',
                                    'gameDateTimeEst': '2025-10-03T22:00:00Z',
                                    'gameLabel': 'Regular Season',
                                    'arenaName': 'Acrisure Arena',
                                    'arenaCity': 'Palm Desert',
                                    'homeTeam': {
                                        'teamId': 1610612747,
                                        'teamName': 'Lakers',
                                        'teamTricode': 'LAL',
                                        'wins': 1,
                                        'losses': 0,
                                        'score': 112,
                                    },
                                    'awayTeam': {
                                        'teamId': 1610612756,
                                        'teamName': 'Suns',
                                        'teamTricode': 'PHX',
                                        'wins': 0,
                                        'losses': 1,
                                        'score': 108,
                                    },
                                },
                                {
                                    'gameId': '0012500009',
                                    'gameStatus': 1,
                                    'gameStatusText': 'Scheduled',
                                    'gameDateEst': '2025-10-03T00:00:00Z',
                                    'gameDateTimeEst': '2025-10-03T05:30:00Z',
                                    'gameLabel': 'Preseason',
                                    'homeTeam': {
                                        'teamId': 1610612740,
                                        'teamName': 'Pelicans',
                                        'teamTricode': 'NOP',
                                    },
                                    'awayTeam': {
                                        'teamId': 50013,
                                        'teamName': 'Phoenix',
                                        'teamTricode': 'SEM',
                                    },
                                },
                            ],
                        }
                    ]
                }
            },
        }

        s3.put_object(
            Bucket='bball-app-nba-data',
            Key='raw/schedule_league_v2/2026/03/07/02/older.json',
            Body=json.dumps(older_payload).encode('utf-8'),
        )
        s3.put_object(
            Bucket='bball-app-nba-data',
            Key='raw/schedule_league_v2/2026/03/07/03/newer.json',
            Body=json.dumps(newer_payload).encode('utf-8'),
        )

        yield s3

        del os.environ['S3_BUCKET_NAME']


class TestGamesRepositoryIntegration:
    def test_upsert_changed_skips_unchanged_items(self, dynamodb_table):
        repo = GamesRepository()

        game_item = {
            'gameId': '0022500001',
            'gameDateEst': '2025-10-03T00:00:00Z',
            'gameDateTimeEst': '2025-10-03T22:00:00Z',
            'gameStatus': 1,
            'gameStatusText': 'Scheduled',
            'homeTeamId': 1610612747,
            'homeTeamName': 'Lakers',
            'homeTeamTricode': 'LAL',
            'awayTeamId': 1610612756,
            'awayTeamName': 'Suns',
            'awayTeamTricode': 'PHX',
        }

        from src.model.models import NbaGame
        game = NbaGame(**game_item)

        assert repo.upsert_changed(game) is True
        assert repo.upsert_changed(game) is False


class TestGamesServiceIntegration:
    def test_consume_games_reads_latest_s3_and_writes_dynamodb(self, s3_bucket_with_schedule, dynamodb_table):
        repo = GamesRepository()
        service = GamesService(repository=repo)

        result = service.consume_games(
            input_options={
                'write_all_season_games': True,
                'include_final_games': True,
            }
        )

        assert result['flattened_games'] == 2
        assert result['mapped_games'] == 1
        assert result['written_games'] == 1

        game = repo.get_by_id('0022500001')
        assert game is not None
        assert game.gameStatus == 3
        assert game.homeTeamScore == 112


class TestGamesLambdaFullExecution:
    def test_lambda_handler_full_nonlive_execution(self, s3_bucket_with_schedule, dynamodb_table):
        response = lambda_handler(
            {
                'input': {
                    'write_all_season_games': True,
                    'include_final_games': True,
                }
            },
            None,
        )
        assert response['statusCode'] == 200

        body = json.loads(response['body'])
        assert body['written_games'] == 1

        repo = GamesRepository()
        persisted = repo.get_all()
        assert len(persisted) == 1
        assert persisted[0].gameId == '0022500001'
