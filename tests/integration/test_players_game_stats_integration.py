"""
Integration tests for player game stats DynamoDB and S3 operations using moto.
"""
import json
import os

import boto3
import pytest
from moto import mock_aws

from src.database.database import DynamoDBConnection
from src.messaging.players_game_stats_handler import lambda_handler
from src.repository.players_game_stats_repository import PlayersGameStatsRepository
from src.service.players_game_stats_service import PlayersGameStatsService


@pytest.fixture
def aws_credentials():
    os.environ['AWS_ACCESS_KEY_ID'] = 'testing'
    os.environ['AWS_SECRET_ACCESS_KEY'] = 'testing'
    os.environ['AWS_SECURITY_TOKEN'] = 'testing'
    os.environ['AWS_SESSION_TOKEN'] = 'testing'
    os.environ['AWS_DEFAULT_REGION'] = 'eu-west-3'


@pytest.fixture
def player_game_stats_table(aws_credentials):
    with mock_aws():
        os.environ['DYNAMODB_TABLE_NAME'] = 'bball-app-data-consumption-players-stats-nonlive'

        dynamodb = boto3.resource('dynamodb', region_name='eu-west-3')
        table = dynamodb.create_table(
            TableName='bball-app-data-consumption-players-stats-nonlive',
            KeySchema=[
                {'AttributeName': 'playerId', 'KeyType': 'HASH'},
                {'AttributeName': 'gameDateGameId', 'KeyType': 'RANGE'},
            ],
            AttributeDefinitions=[
                {'AttributeName': 'playerId', 'AttributeType': 'N'},
                {'AttributeName': 'gameDateGameId', 'AttributeType': 'S'},
                {'AttributeName': 'gameId', 'AttributeType': 'S'},
                {'AttributeName': 'pts', 'AttributeType': 'N'},
            ],
            GlobalSecondaryIndexes=[
                {
                    'IndexName': 'byGameIdPts',
                    'KeySchema': [
                        {'AttributeName': 'gameId', 'KeyType': 'HASH'},
                        {'AttributeName': 'pts', 'KeyType': 'RANGE'},
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
def s3_bucket_with_player_game_logs(aws_credentials, player_game_stats_table):
    os.environ['S3_BUCKET_NAME'] = 'bball-app-nba-data'

    s3 = boto3.client('s3', region_name='eu-west-3')
    s3.create_bucket(
        Bucket='bball-app-nba-data',
        CreateBucketConfiguration={'LocationConstraint': 'eu-west-3'},
    )

    older_payload = {
        'source': 'nba_api',
        'endpoint': 'player_game_logs',
        'fetched_at_utc': '2026-03-10T10:40:12.533658+00:00',
        'aws_account_id': '590183661886',
        'schema_version': 'v1',
        'ingestion_id': 'older-ingestion-id',
        'params': {
            'season': '2025-26',
            'date_from': '03/07/2026',
            'date_to': '03/07/2026',
        },
        'payload': {
            'resource': 'gamelogs',
            'parameters': {
                'MeasureType': 'Base',
                'PerMode': 'Totals',
                'LeagueID': '00',
                'SeasonYear': '2025-26',
                'SeasonType': None,
                'PORound': None,
                'TeamID': 0,
                'PlayerID': 0,
                'Outcome': None,
                'Location': None,
                'Month': None,
                'SeasonSegment': None,
                'DateFrom': '03/07/2026',
                'DateTo': '03/07/2026',
                'OppTeamID': None,
                'VsConference': None,
                'VsDivision': None,
                'GameSegment': None,
                'Period': None,
                'ShotClockRange': None,
                'LastNGames': None,
                'ISTRound': None,
            },
            'resultSets': [
                {
                    'name': 'PlayerGameLogs',
                    'headers': [
                        'SEASON_YEAR', 'PLAYER_ID', 'PLAYER_NAME', 'NICKNAME', 'TEAM_ID',
                        'TEAM_ABBREVIATION', 'TEAM_NAME', 'GAME_ID', 'GAME_DATE', 'MATCHUP',
                        'WL', 'MIN', 'FGM', 'FGA', 'FG_PCT', 'FG3M', 'FG3A', 'FG3_PCT',
                        'FTM', 'FTA', 'FT_PCT', 'OREB', 'DREB', 'REB', 'AST', 'TOV', 'STL',
                        'BLK', 'BLKA', 'PF', 'PFD', 'PTS', 'PLUS_MINUS', 'NBA_FANTASY_PTS',
                        'DD2', 'TD3', 'WNBA_FANTASY_PTS', 'AVAILABLE_FLAG', 'MIN_SEC', 'TEAM_COUNT'
                    ],
                    'rowSet': [
                        [
                            '2025-26', 1630552, 'Jalen Johnson', 'Jalen', 1610612737, 'ATL',
                            'Atlanta Hawks', '0022500917', '2026-03-07T00:00:00', 'ATL vs. PHI',
                            'W', '38:40', 12, 19, 0.632, 2, 4, 0.5, 9, 9, 1.0, 2, 8, 10, 7,
                            5, 2, 0, 1, 2, 6, 35, 12, 58.5, 1, 0, 58.0, 1, '38:40', 1
                        ]
                    ],
                }
            ],
        },
    }

    newer_payload = {
        'source': 'nba_api',
        'endpoint': 'player_game_logs',
        'fetched_at_utc': '2026-03-10T11:40:12.533658+00:00',
        'aws_account_id': '590183661886',
        'schema_version': 'v1',
        'ingestion_id': 'newer-ingestion-id',
        'params': {
            'season': '2025-26',
            'date_from': '03/07/2026',
            'date_to': '03/07/2026',
        },
        'payload': {
            'resource': 'gamelogs',
            'parameters': {
                'MeasureType': 'Base',
                'PerMode': 'Totals',
                'LeagueID': '00',
                'SeasonYear': '2025-26',
                'SeasonType': None,
                'PORound': None,
                'TeamID': 0,
                'PlayerID': 0,
                'Outcome': None,
                'Location': None,
                'Month': None,
                'SeasonSegment': None,
                'DateFrom': '03/07/2026',
                'DateTo': '03/07/2026',
                'OppTeamID': None,
                'VsConference': None,
                'VsDivision': None,
                'GameSegment': None,
                'Period': None,
                'ShotClockRange': None,
                'LastNGames': None,
                'ISTRound': None,
            },
            'resultSets': [
                {
                    'name': 'PlayerGameLogs',
                    'headers': [
                        'SEASON_YEAR', 'PLAYER_ID', 'PLAYER_NAME', 'NICKNAME', 'TEAM_ID',
                        'TEAM_ABBREVIATION', 'TEAM_NAME', 'GAME_ID', 'GAME_DATE', 'MATCHUP',
                        'WL', 'MIN', 'FGM', 'FGA', 'FG_PCT', 'FG3M', 'FG3A', 'FG3_PCT',
                        'FTM', 'FTA', 'FT_PCT', 'OREB', 'DREB', 'REB', 'AST', 'TOV', 'STL',
                        'BLK', 'BLKA', 'PF', 'PFD', 'PTS', 'PLUS_MINUS', 'NBA_FANTASY_PTS',
                        'DD2', 'TD3', 'WNBA_FANTASY_PTS', 'AVAILABLE_FLAG', 'MIN_SEC', 'TEAM_COUNT'
                    ],
                    'rowSet': [
                        [
                            '2025-26', 1630552, 'Jalen Johnson', 'Jalen', 1610612737, 'ATL',
                            'Atlanta Hawks', '0022500917', '2026-03-07T00:00:00', 'ATL vs. PHI',
                            'W', '38:40', 13, 20, 0.65, 2, 4, 0.5, 9, 9, 1.0, 2, 8, 10, 7,
                            5, 2, 0, 1, 2, 6, 37, 14, 60.0, 1, 0, 60.0, 1, '38:40', 1
                        ],
                        [
                            '2025-26', 1629008, 'Michael Porter Jr.', 'Michael', 1610612751, 'BKN',
                            'Brooklyn Nets', '0022500916', '2026-03-07T00:00:00', 'BKN @ DET',
                            'W', '39:17', 10, 25, 0.4, 3, 12, 0.25, 7, 7, 1.0, 2, 11, 13, 1,
                            2, 2, 1, 1, 2, 9, 30, 13, 54.1, 1, 0, 53.0, 1, '39:17', 1
                        ],
                    ],
                }
            ],
        },
    }

    s3.put_object(
        Bucket='bball-app-nba-data',
        Key='raw/player_game_logs/2026/03/10/10/older.json',
        Body=json.dumps(older_payload).encode('utf-8'),
    )
    s3.put_object(
        Bucket='bball-app-nba-data',
        Key='raw/player_game_logs/2026/03/10/11/newer.json',
        Body=json.dumps(newer_payload).encode('utf-8'),
    )

    yield s3

    del os.environ['S3_BUCKET_NAME']


class TestPlayersGameStatsRepositoryIntegration:
    def test_upsert_changed_skips_unchanged_items(self, player_game_stats_table):
        repo = PlayersGameStatsRepository()

        from src.model.models import NbaPlayerGameLog

        log = NbaPlayerGameLog(
            playerId=1630552,
            playerName='Jalen Johnson',
            nickname='Jalen',
            teamId=1610612737,
            teamAbbreviation='ATL',
            teamName='Atlanta Hawks',
            gameId='0022500917',
            gameDate='2026-03-07',
            matchup='ATL vs. PHI',
            winLoss='W',
            minutes='38:40',
            seasonYear='2025-26',
            fgm=13,
            fga=20,
            fgPct=0.65,
            fg3m=2,
            fg3a=4,
            fg3Pct=0.5,
            ftm=9,
            fta=9,
            ftPct=1.0,
            oreb=2,
            dreb=8,
            reb=10,
            ast=7,
            tov=5,
            stl=2,
            blk=0,
            blka=1,
            pf=2,
            pfd=6,
            pts=37,
            plusMinus=14,
            nbaFantasyPts=60.0,
            dd2=1,
            td3=0,
            wnbaFantasyPts=60.0,
            availableFlag=1,
            minSec='38:40',
            teamCount=1,
        )

        assert repo.upsert_changed(log) is True
        assert repo.upsert_changed(log) is False


class TestPlayersGameStatsServiceIntegration:
    def test_consume_player_game_logs_reads_latest_s3_and_writes_dynamodb(self, s3_bucket_with_player_game_logs, player_game_stats_table):
        repo = PlayersGameStatsRepository()
        service = PlayersGameStatsService(repository=repo)

        result = service.consume_player_game_logs()

        assert result['flattened_player_game_logs'] == 2
        assert result['mapped_player_game_logs'] == 2
        assert result['written_player_game_logs'] == 2

        game_log = repo.get_by_player_and_game(1630552, '2026-03-07#0022500917')
        assert game_log is not None
        assert game_log.pts == 37
        assert game_log.teamAbbreviation == 'ATL'


class TestPlayersGameStatsLambdaFullExecution:
    def test_lambda_handler_full_nonlive_execution(self, s3_bucket_with_player_game_logs, player_game_stats_table):
        response = lambda_handler({}, None)
        assert response['statusCode'] == 200

        body = json.loads(response['body'])
        assert body['written_player_game_logs'] == 2

        repo = PlayersGameStatsRepository()
        persisted = repo.get_all()
        assert len(persisted) == 2
        assert persisted[0].gameId == '0022500917'
