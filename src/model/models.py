"""
Database models for the NBA teams data consumption application.
"""
from dataclasses import dataclass
from typing import Optional


@dataclass
class NbaTeam:
    """
    NBA team model matching the teams-static DynamoDB table structure.
    """
    teamId: int = 0
    fullName: str = ""
    abbreviation: str = ""
    nickname: str = ""

    def to_dict(self) -> dict:
        """Convert model to dictionary."""
        return {
            'teamId': self.teamId,
            'fullName': self.fullName,
            'abbreviation': self.abbreviation,
            'nickname': self.nickname,
        }

    @classmethod
    def from_raw(cls, raw: dict) -> 'NbaTeam':
        """
        Map a raw teams_static dict from nba_api / S3 to an NbaTeam instance.

        Args:
            raw: Raw dict with keys id, full_name, abbreviation, nickname.

        Returns:
            NbaTeam instance.
        """
        return cls(
            teamId=int(raw['id']),
            fullName=raw['full_name'],
            abbreviation=raw['abbreviation'],
            nickname=raw['nickname'],
        )


@dataclass
class NbaGame:
    """NBA game model matching the games DynamoDB table structure."""

    gameId: str = ""
    leagueKey: str = "NBA"
    gameDateEst: str = ""
    gameDateTimeEst: str = ""
    gameStatus: int = 0
    gameStatusText: str = ""
    homeTeamId: int = 0
    homeTeamName: str = ""
    homeTeamTricode: str = ""
    awayTeamId: int = 0
    awayTeamName: str = ""
    awayTeamTricode: str = ""
    homeTeamWins: Optional[int] = None
    homeTeamLosses: Optional[int] = None
    homeTeamScore: Optional[int] = None
    awayTeamWins: Optional[int] = None
    awayTeamLosses: Optional[int] = None
    awayTeamScore: Optional[int] = None
    arenaName: Optional[str] = None
    arenaCity: Optional[str] = None
    dataHash: Optional[str] = None

    def to_dict(self) -> dict:
        """Convert model to a sparse dictionary for persistence."""
        item = {
            'gameId': self.gameId,
            'leagueKey': self.leagueKey,
            'gameDateEst': self.gameDateEst,
            'gameDateTimeEst': self.gameDateTimeEst,
            'gameStatus': self.gameStatus,
            'gameStatusText': self.gameStatusText.strip(),
            'homeTeamId': self.homeTeamId,
            'homeTeamName': self.homeTeamName,
            'homeTeamTricode': self.homeTeamTricode,
            'awayTeamId': self.awayTeamId,
            'awayTeamName': self.awayTeamName,
            'awayTeamTricode': self.awayTeamTricode,
        }
        if self.homeTeamWins is not None:
            item['homeTeamWins'] = self.homeTeamWins
        if self.homeTeamLosses is not None:
            item['homeTeamLosses'] = self.homeTeamLosses
        if self.homeTeamScore is not None:
            item['homeTeamScore'] = self.homeTeamScore
        if self.awayTeamWins is not None:
            item['awayTeamWins'] = self.awayTeamWins
        if self.awayTeamLosses is not None:
            item['awayTeamLosses'] = self.awayTeamLosses
        if self.awayTeamScore is not None:
            item['awayTeamScore'] = self.awayTeamScore
        if self.arenaName:
            item['arenaName'] = self.arenaName
        if self.arenaCity:
            item['arenaCity'] = self.arenaCity
        if self.dataHash:
            item['dataHash'] = self.dataHash
        return item

    @classmethod
    def from_raw(cls, raw: dict) -> 'NbaGame':
        """Map a raw schedule_league_v2 game dict to an NbaGame instance."""
        return cls(
            gameId=str(raw['gameId']),
            leagueKey='NBA',
            gameDateEst=raw['gameDateEst'],
            gameDateTimeEst=raw['gameDateTimeEst'],
            gameStatus=int(raw['gameStatus']),
            gameStatusText=str(raw['gameStatusText']).strip(),
            homeTeamId=int(raw['homeTeam']['teamId']),
            homeTeamName=raw['homeTeam']['teamName'],
            homeTeamTricode=raw['homeTeam']['teamTricode'],
            awayTeamId=int(raw['awayTeam']['teamId']),
            awayTeamName=raw['awayTeam']['teamName'],
            awayTeamTricode=raw['awayTeam']['teamTricode'],
            homeTeamWins=_to_optional_int(raw.get('homeTeam', {}).get('wins')),
            homeTeamLosses=_to_optional_int(raw.get('homeTeam', {}).get('losses')),
            homeTeamScore=_to_optional_int(raw.get('homeTeam', {}).get('score')),
            awayTeamWins=_to_optional_int(raw.get('awayTeam', {}).get('wins')),
            awayTeamLosses=_to_optional_int(raw.get('awayTeam', {}).get('losses')),
            awayTeamScore=_to_optional_int(raw.get('awayTeam', {}).get('score')),
            arenaName=raw.get('arenaName') or None,
            arenaCity=raw.get('arenaCity') or None,
        )


def _to_optional_int(value) -> Optional[int]:
    if value is None:
        return None
    return int(value)


def _to_optional_float(value) -> Optional[float]:
    if value is None or value == '':
        return None
    if isinstance(value, str) and ':' in value:
        minutes_str, seconds_str = value.split(':', 1)
        return float(minutes_str) + (float(seconds_str) / 60.0)
    return float(value)


@dataclass
class NbaPlayer:
    """NBA player model matching the players_index DynamoDB table structure.

    Field set follows the contract documented in
    bball-app-nba_api_client/docs/DATABASE_SCHEMA.md (Table 3: nba_players).
    Stats (PTS/REB/AST) and historical metadata (DRAFT_*, FROM_YEAR, TO_YEAR,
    WEIGHT, COLLEGE) are intentionally excluded; they belong to future tables
    (`nba_player_stats`) or are deferred. `injuryStatus` will be added when the
    injuries pipeline lands and will be denormalized here for fast roster reads.
    """

    playerId: int = 0
    firstName: str = ""
    lastName: str = ""
    displayName: str = ""
    teamId: int = 0
    teamName: str = ""
    teamAbbreviation: str = ""
    position: str = ""
    jerseyNumber: Optional[str] = None
    height: str = ""
    country: str = ""
    rosterStatus: int = 0
    dataHash: Optional[str] = None

    def to_dict(self) -> dict:
        """Convert model to a sparse dictionary for persistence."""
        item = {
            'playerId': self.playerId,
            'firstName': self.firstName,
            'lastName': self.lastName,
            'displayName': self.displayName,
            'teamId': self.teamId,
            'teamName': self.teamName,
            'teamAbbreviation': self.teamAbbreviation,
            'position': self.position,
            'height': self.height,
            'country': self.country,
            'rosterStatus': self.rosterStatus,
        }
        if self.jerseyNumber is not None and self.jerseyNumber != "":
            item['jerseyNumber'] = self.jerseyNumber
        if self.dataHash:
            item['dataHash'] = self.dataHash
        return item

    @classmethod
    def from_raw(cls, raw: dict) -> 'NbaPlayer':
        """
        Map a raw player_index dict from nba_api / S3 to an NbaPlayer instance.

        Args:
            raw: Raw dict with keys PERSON_ID, PLAYER_FIRST_NAME, PLAYER_LAST_NAME, etc.

        Returns:
            NbaPlayer instance.
        """
        first_name = str(raw.get('PLAYER_FIRST_NAME', '')).strip()
        last_name = raw['PLAYER_LAST_NAME'].strip()
        full_name = f"{first_name} {last_name}".strip()
        jersey_raw = raw.get('JERSEY_NUMBER')
        jersey = str(jersey_raw).strip() if jersey_raw not in (None, '') else None
        return cls(
            playerId=int(raw['PERSON_ID']),
            firstName=first_name,
            lastName=last_name,
            displayName=full_name,
            teamId=int(raw.get('TEAM_ID') or 0),
            teamName=str(raw.get('TEAM_NAME') or '').strip(),
            teamAbbreviation=str(raw.get('TEAM_ABBREVIATION') or '').strip(),
            position=str(raw.get('POSITION', '') or '').strip(),
            jerseyNumber=jersey,
            height=str(raw.get('HEIGHT', '') or '').strip(),
            country=str(raw.get('COUNTRY', '') or '').strip(),
            rosterStatus=int(raw.get('ROSTER_STATUS') or 0),
        )


@dataclass
class NbaPlayerGameLog:
    """NBA player game log model matching the player-game-logs DynamoDB table."""

    playerId: int = 0
    playerName: str = ""
    nickname: str = ""
    teamId: int = 0
    teamAbbreviation: str = ""
    teamName: str = ""
    gameId: str = ""
    gameDate: str = ""
    matchup: str = ""
    winLoss: str = ""
    minutes: str = ""
    minutesDecimal: Optional[float] = None
    seasonYear: str = ""
    fgm: int = 0
    fga: int = 0
    fgPct: Optional[float] = None
    fg3m: int = 0
    fg3a: int = 0
    fg3Pct: Optional[float] = None
    ftm: int = 0
    fta: int = 0
    ftPct: Optional[float] = None
    oreb: int = 0
    dreb: int = 0
    reb: int = 0
    ast: int = 0
    tov: int = 0
    stl: int = 0
    blk: int = 0
    blka: int = 0
    pf: int = 0
    pfd: int = 0
    pts: int = 0
    plusMinus: Optional[float] = None
    nbaFantasyPts: Optional[float] = None
    dd2: int = 0
    td3: int = 0
    wnbaFantasyPts: Optional[float] = None
    availableFlag: int = 0
    minSec: str = ""
    teamCount: int = 0
    dataHash: Optional[str] = None

    def to_dict(self) -> dict:
        """Convert model to a sparse dictionary for persistence."""
        item = {
            'playerId': self.playerId,
            'playerName': self.playerName,
            'nickname': self.nickname,
            'teamId': self.teamId,
            'teamAbbreviation': self.teamAbbreviation,
            'teamName': self.teamName,
            'gameId': self.gameId,
            'gameDateGameId': f'{self.gameDate}#{self.gameId}',
            'gameDate': self.gameDate,
            'matchup': self.matchup,
            'winLoss': self.winLoss,
            'minutes': self.minutes,
            'seasonYear': self.seasonYear,
            'fgm': self.fgm,
            'fga': self.fga,
            'fg3m': self.fg3m,
            'fg3a': self.fg3a,
            'ftm': self.ftm,
            'fta': self.fta,
            'oreb': self.oreb,
            'dreb': self.dreb,
            'reb': self.reb,
            'ast': self.ast,
            'tov': self.tov,
            'stl': self.stl,
            'blk': self.blk,
            'blka': self.blka,
            'pf': self.pf,
            'pfd': self.pfd,
            'pts': self.pts,
            'dd2': self.dd2,
            'td3': self.td3,
            'availableFlag': self.availableFlag,
            'minSec': self.minSec,
            'teamCount': self.teamCount,
        }
        if self.minutesDecimal is not None:
            item['minutesDecimal'] = self.minutesDecimal
        if self.fgPct is not None:
            item['fgPct'] = self.fgPct
        if self.fg3Pct is not None:
            item['fg3Pct'] = self.fg3Pct
        if self.ftPct is not None:
            item['ftPct'] = self.ftPct
        if self.plusMinus is not None:
            item['plusMinus'] = self.plusMinus
        if self.nbaFantasyPts is not None:
            item['nbaFantasyPts'] = self.nbaFantasyPts
        if self.wnbaFantasyPts is not None:
            item['wnbaFantasyPts'] = self.wnbaFantasyPts
        if self.dataHash:
            item['dataHash'] = self.dataHash
        return item

    @classmethod
    def from_raw(cls, raw: dict) -> 'NbaPlayerGameLog':
        """Map a raw PlayerGameLogs row to an NbaPlayerGameLog instance."""
        game_date_raw = str(raw['GAME_DATE']).strip()
        game_date = game_date_raw.split('T', 1)[0] if 'T' in game_date_raw else game_date_raw
        return cls(
            playerId=int(raw['PLAYER_ID']),
            playerName=str(raw.get('PLAYER_NAME', '')).strip(),
            nickname=str(raw.get('NICKNAME', '')).strip(),
            teamId=int(raw['TEAM_ID']),
            teamAbbreviation=str(raw.get('TEAM_ABBREVIATION', '')).strip(),
            teamName=str(raw.get('TEAM_NAME', '')).strip(),
            gameId=str(raw['GAME_ID']).strip(),
            gameDate=game_date,
            matchup=str(raw.get('MATCHUP', '')).strip(),
            winLoss=str(raw.get('WL', '')).strip(),
            minutes=str(raw.get('MIN', '')).strip(),
            minutesDecimal=_to_optional_float(raw.get('MIN')),
            seasonYear=str(raw.get('SEASON_YEAR', '')).strip(),
            fgm=int(raw.get('FGM') or 0),
            fga=int(raw.get('FGA') or 0),
            fgPct=_to_optional_float(raw.get('FG_PCT')),
            fg3m=int(raw.get('FG3M') or 0),
            fg3a=int(raw.get('FG3A') or 0),
            fg3Pct=_to_optional_float(raw.get('FG3_PCT')),
            ftm=int(raw.get('FTM') or 0),
            fta=int(raw.get('FTA') or 0),
            ftPct=_to_optional_float(raw.get('FT_PCT')),
            oreb=int(raw.get('OREB') or 0),
            dreb=int(raw.get('DREB') or 0),
            reb=int(raw.get('REB') or 0),
            ast=int(raw.get('AST') or 0),
            tov=int(raw.get('TOV') or 0),
            stl=int(raw.get('STL') or 0),
            blk=int(raw.get('BLK') or 0),
            blka=int(raw.get('BLKA') or 0),
            pf=int(raw.get('PF') or 0),
            pfd=int(raw.get('PFD') or 0),
            pts=int(raw.get('PTS') or 0),
            plusMinus=_to_optional_float(raw.get('PLUS_MINUS')),
            nbaFantasyPts=_to_optional_float(raw.get('NBA_FANTASY_PTS')),
            dd2=int(raw.get('DD2') or 0),
            td3=int(raw.get('TD3') or 0),
            wnbaFantasyPts=_to_optional_float(raw.get('WNBA_FANTASY_PTS')),
            availableFlag=int(raw.get('AVAILABLE_FLAG') or 0),
            minSec=str(raw.get('MIN_SEC', '')).strip(),
            teamCount=int(raw.get('TEAM_COUNT') or 0),
        )


@dataclass
class NbaPlayerInjury:
    """NBA player injury model matching the players_injuries DynamoDB table.

    Field set follows the contract documented in
    bball-app-nba_api_client/docs/DATABASE_SCHEMA.md (Table 6: nba_injuries).
    Uses a composite key (playerId hash + injuryKey range) to keep a daily
    history of reports without collisions. `injuryKey` is `reportDate#fetchedAt`.
    """

    playerId: int = 0
    injuryKey: str = ""
    playerName: str = ""
    teamAbbr: str = ""
    status: str = ""
    availability: str = ""
    reasonType: str = ""
    reason: str = ""
    reportDate: str = ""
    fetchedAt: str = ""
    updatedAt: Optional[str] = None
    dataHash: Optional[str] = None

    def to_dict(self) -> dict:
        item = {
            'playerId': self.playerId,
            'injuryKey': self.injuryKey,
            'playerName': self.playerName,
            'teamAbbr': self.teamAbbr,
            'status': self.status,
            'availability': self.availability,
            'reasonType': self.reasonType,
            'reason': self.reason,
            'reportDate': self.reportDate,
            'fetchedAt': self.fetchedAt,
        }
        if self.updatedAt:
            item['updatedAt'] = self.updatedAt
        if self.dataHash:
            item['dataHash'] = self.dataHash
        return item

    @staticmethod
    def _normalize_report_date(report_date) -> str:
        """Convert MM/DD/YYYY (US source) to ISO YYYY-MM-DD; pass through ISO."""
        if not report_date:
            return ""
        text = str(report_date).strip()
        if '/' in text:
            parts = text.split('/')
            if len(parts) == 3:
                month, day, year = parts
                return f"{int(year):04d}-{int(month):02d}-{int(day):02d}"
        return text

    @classmethod
    def from_raw(cls, raw: dict, fetched_at: str, updated_at: Optional[str] = None) -> 'NbaPlayerInjury':
        """Map a raw injury entry from the injury_report payload.

        Args:
            raw: One element of payload.injuries[].
            fetched_at: Envelope `fetched_at_utc` (ISO string).
            updated_at: Optional `payload.updated_at` (ISO string).

        Returns:
            NbaPlayerInjury instance.

        Raises:
            ValueError: If `player_id` is missing or not coercible to int.
        """
        player_id_raw = raw.get('player_id')
        if player_id_raw is None:
            raise ValueError('player_id is required')
        report_date_iso = cls._normalize_report_date(raw.get('report_date'))
        return cls(
            playerId=int(player_id_raw),
            injuryKey=f"{report_date_iso}#{fetched_at}",
            playerName=str(raw.get('player_name') or '').strip(),
            teamAbbr=str(raw.get('team_abbr') or '').strip(),
            status=str(raw.get('status') or '').strip(),
            availability=str(raw.get('availability') or '').strip(),
            reasonType=str(raw.get('reason_type') or '').strip(),
            reason=str(raw.get('reason') or '').strip(),
            reportDate=report_date_iso,
            fetchedAt=fetched_at,
            updatedAt=updated_at,
        )
