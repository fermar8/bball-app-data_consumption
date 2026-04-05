"""
Unit tests for the service layer with mocked dependencies.
"""
import pytest
from unittest.mock import MagicMock, patch

from src.model.models import NbaTeam
from src.service.service import TeamsConsumptionService, map_teams


RAW_TEAMS = [
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
]


class TestMapTeams:
    """Unit tests for the map_teams helper."""

    def test_maps_all_valid_records(self):
        teams = map_teams(RAW_TEAMS)
        assert len(teams) == 2
        assert teams[0].teamId == 1610612737
        assert teams[1].teamId == 1610612738

    def test_skips_malformed_records(self):
        """Malformed records (missing id) should be skipped with a warning."""
        raw = [{'full_name': 'Bad Team'}] + RAW_TEAMS
        teams = map_teams(raw)
        assert len(teams) == 2

    def test_returns_empty_list_for_empty_input(self):
        assert map_teams([]) == []

    def test_returns_nba_team_instances(self):
        teams = map_teams(RAW_TEAMS)
        assert all(isinstance(t, NbaTeam) for t in teams)


class TestTeamsConsumptionService:
    """Unit tests for TeamsConsumptionService with mocked repository."""

    @pytest.fixture
    def mock_repository(self):
        repo = MagicMock()
        repo.upsert_batch.return_value = 2
        return repo

    @pytest.fixture
    def service(self, mock_repository):
        return TeamsConsumptionService(repository=mock_repository)

    @patch('src.service.service._fetch_latest_teams_payload', return_value=RAW_TEAMS)
    @patch('src.service.service._get_bucket_name', return_value='test-bucket')
    def test_consume_teams_returns_count(self, mock_bucket, mock_fetch, service):
        """consume_teams should return the number of teams persisted."""
        count = service.consume_teams()
        assert count == 2

    @patch('src.service.service._fetch_latest_teams_payload', return_value=RAW_TEAMS)
    @patch('src.service.service._get_bucket_name', return_value='test-bucket')
    def test_consume_teams_calls_upsert_batch(self, mock_bucket, mock_fetch, service, mock_repository):
        """consume_teams should call upsert_batch with mapped NbaTeam objects."""
        service.consume_teams()
        mock_repository.upsert_batch.assert_called_once()
        persisted = mock_repository.upsert_batch.call_args[0][0]
        assert len(persisted) == 2
        assert isinstance(persisted[0], NbaTeam)

    @patch('src.service.service._get_bucket_name', side_effect=ValueError("S3_BUCKET_NAME not set"))
    def test_consume_teams_raises_on_missing_bucket(self, mock_bucket, service):
        """consume_teams should propagate ValueError when S3_BUCKET_NAME is missing."""
        with pytest.raises(ValueError, match="S3_BUCKET_NAME not set"):
            service.consume_teams()

    @patch(
        'src.service.service._fetch_latest_teams_payload',
        side_effect=FileNotFoundError("No files found"),
    )
    @patch('src.service.service._get_bucket_name', return_value='test-bucket')
    def test_consume_teams_raises_on_missing_s3_data(self, mock_bucket, mock_fetch, service):
        """consume_teams should propagate FileNotFoundError when S3 has no data."""
        with pytest.raises(FileNotFoundError):
            service.consume_teams()
