"""
Integration tests for teams-static DynamoDB and S3 operations using moto.
"""
import json
import os
import pytest
import boto3
from moto import mock_aws

from src.database.database import DynamoDBConnection
from src.model.models import NbaTeam
from src.repository.teams_static_repository import TeamsStaticRepository
from src.service.teams_static_service import TeamsStaticService


@pytest.fixture
def aws_credentials():
    """Mock AWS credentials for moto."""
    os.environ['AWS_ACCESS_KEY_ID'] = 'testing'
    os.environ['AWS_SECRET_ACCESS_KEY'] = 'testing'
    os.environ['AWS_SECURITY_TOKEN'] = 'testing'
    os.environ['AWS_SESSION_TOKEN'] = 'testing'
    os.environ['AWS_DEFAULT_REGION'] = 'eu-west-3'


@pytest.fixture
def dynamodb_table(aws_credentials):
    """Create a mock DynamoDB teams-static table for testing."""
    with mock_aws():
        os.environ['DYNAMODB_TABLE_NAME'] = 'teams-static-nonlive'

        dynamodb = boto3.resource('dynamodb', region_name='eu-west-3')
        table = dynamodb.create_table(
            TableName='teams-static-nonlive',
            KeySchema=[
                {'AttributeName': 'teamId', 'KeyType': 'HASH'},
            ],
            AttributeDefinitions=[
                {'AttributeName': 'teamId', 'AttributeType': 'N'},
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
def s3_bucket_with_teams(aws_credentials, dynamodb_table):
    """Create a mock S3 bucket with a teams_static raw payload."""
    with mock_aws():
        os.environ['S3_BUCKET_NAME'] = 'bball-app-nba-data'

        s3 = boto3.client('s3', region_name='eu-west-3')
        s3.create_bucket(
            Bucket='bball-app-nba-data',
            CreateBucketConfiguration={'LocationConstraint': 'eu-west-3'},
        )

        payload_body = json.dumps({
            'source': 'nba_api',
            'endpoint': 'teams_static',
            'fetched_at_utc': '2026-03-08T13:09:01.492795+00:00Z',
            'aws_account_id': '590183661886',
            'params': {},
            'payload': [
                {
                    'id': 1610612737,
                    'full_name': 'Atlanta Hawks',
                    'abbreviation': 'ATL',
                    'nickname': 'Hawks',
                    'city': 'Atlanta',
                    'state': 'Georgia',
                    'year_founded': 1949,
                },
                {
                    'id': 1610612738,
                    'full_name': 'Boston Celtics',
                    'abbreviation': 'BOS',
                    'nickname': 'Celtics',
                    'city': 'Boston',
                    'state': 'Massachusetts',
                    'year_founded': 1946,
                },
            ],
        })

        s3.put_object(
            Bucket='bball-app-nba-data',
            Key='raw/teams_static/2025/01/15/10/20250115T100000Z_abc12345.json',
            Body=payload_body.encode('utf-8'),
        )

        yield s3

        del os.environ['S3_BUCKET_NAME']


class TestTeamsStaticRepositoryIntegration:
    """Integration tests for TeamsStaticRepository with real DynamoDB (mocked via moto)."""

    def test_upsert_single_team(self, dynamodb_table):
        """Upserting a team should persist it in DynamoDB."""
        repo = TeamsStaticRepository()
        team = NbaTeam(
            teamId=1610612737,
            fullName='Atlanta Hawks',
            abbreviation='ATL',
            nickname='Hawks',
        )
        result = repo.upsert(team)

        assert result.teamId == 1610612737
        assert result.fullName == 'Atlanta Hawks'

    def test_get_by_id_returns_persisted_team(self, dynamodb_table):
        """get_by_id should return the team written by upsert."""
        repo = TeamsStaticRepository()
        team = NbaTeam(
            teamId=1610612737,
            fullName='Atlanta Hawks',
            abbreviation='ATL',
            nickname='Hawks',
        )
        repo.upsert(team)

        fetched = repo.get_by_id(1610612737)
        assert fetched is not None
        assert fetched.teamId == 1610612737
        assert fetched.fullName == 'Atlanta Hawks'
        assert fetched.abbreviation == 'ATL'

    def test_get_by_id_returns_none_for_missing_team(self, dynamodb_table):
        """get_by_id should return None for a non-existent teamId."""
        repo = TeamsStaticRepository()
        assert repo.get_by_id(999999) is None

    def test_upsert_batch_persists_all_teams(self, dynamodb_table):
        """upsert_batch should persist every team in the list."""
        repo = TeamsStaticRepository()
        teams = [
            NbaTeam(teamId=1610612737, fullName='Atlanta Hawks', abbreviation='ATL',
                nickname='Hawks'),
            NbaTeam(teamId=1610612738, fullName='Boston Celtics', abbreviation='BOS',
                nickname='Celtics'),
        ]
        count = repo.upsert_batch(teams)

        assert count == 2
        assert repo.get_by_id(1610612737) is not None
        assert repo.get_by_id(1610612738) is not None

    def test_get_all_returns_all_persisted_teams(self, dynamodb_table):
        """get_all should return every team in the table."""
        repo = TeamsStaticRepository()
        teams = [
            NbaTeam(teamId=1610612737, fullName='Atlanta Hawks', abbreviation='ATL',
                nickname='Hawks'),
            NbaTeam(teamId=1610612738, fullName='Boston Celtics', abbreviation='BOS',
                nickname='Celtics'),
        ]
        repo.upsert_batch(teams)

        all_teams = repo.get_all()
        assert len(all_teams) == 2
        team_ids = {t.teamId for t in all_teams}
        assert 1610612737 in team_ids
        assert 1610612738 in team_ids

    def test_upsert_overwrites_existing_team(self, dynamodb_table):
        """Upserting the same teamId twice should overwrite the record."""
        repo = TeamsStaticRepository()
        original = NbaTeam(teamId=1610612737, fullName='Atlanta Hawks',
                           abbreviation='ATL', nickname='Hawks')
        repo.upsert(original)

        updated = NbaTeam(teamId=1610612737, fullName='Atlanta Hawks (Updated)',
                          abbreviation='ATL', nickname='Hawks')
        repo.upsert(updated)

        fetched = repo.get_by_id(1610612737)
        assert fetched.fullName == 'Atlanta Hawks (Updated)'


class TestTeamsStaticServiceIntegration:
    """Integration tests for TeamsStaticService against mocked S3 + DynamoDB."""

    def test_consume_teams_reads_s3_and_writes_dynamodb(self, s3_bucket_with_teams, dynamodb_table):
        """
        Full integration: consume_teams should read from S3 and persist to DynamoDB.
        The dynamodb_table fixture is needed to initialise the table;
        s3_bucket_with_teams nests dynamodb_table so both are active.
        """
        repo = TeamsStaticRepository()
        service = TeamsStaticService(repository=repo)

        count = service.consume_teams()

        assert count == 2
        team = repo.get_by_id(1610612737)
        assert team is not None
        assert team.fullName == 'Atlanta Hawks'

        team2 = repo.get_by_id(1610612738)
        assert team2 is not None
        assert team2.abbreviation == 'BOS'

    def test_consume_teams_returns_zero_for_empty_payload(self, aws_credentials, dynamodb_table):
        """When S3 payload list is empty no records are written."""
        with mock_aws():
            os.environ['S3_BUCKET_NAME'] = 'bball-app-nba-data'

            s3 = boto3.client('s3', region_name='eu-west-3')
            s3.create_bucket(
                Bucket='bball-app-nba-data',
                CreateBucketConfiguration={'LocationConstraint': 'eu-west-3'},
            )
            s3.put_object(
                Bucket='bball-app-nba-data',
                Key='raw/teams_static/2025/01/15/10/20250115T100000Z_abc12345.json',
                Body=json.dumps({
                    'source': 'nba_api',
                    'endpoint': 'teams_static',
                    'fetched_at_utc': '2026-03-08T13:09:01.492795+00:00Z',
                    'aws_account_id': '590183661886',
                    'params': {},
                    'payload': [],
                }).encode(),
            )

            repo = TeamsStaticRepository()
            service = TeamsStaticService(repository=repo)
            count = service.consume_teams()

            assert count == 0
            del os.environ['S3_BUCKET_NAME']
