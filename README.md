# bball-app-data_consumption

Data consumption Lambdas that read raw NBA data from S3, validate it, and persist it to DynamoDB.

## Architecture

```
S3 (raw/)  ──>  Lambda  ──>  DynamoDB
                  │
           schema validation
           (jsonschema)
```

Each business domain (e.g. teams-static) has its own Lambda, service, repository, and DynamoDB table. The project is structured so new domains can be added by following the same pattern.

## Project Structure

```
src/
  messaging/          # Lambda handlers (one per domain)
  service/            # Business logic + S3 fetching
  repository/         # DynamoDB persistence
  model/              # Dataclasses (shared across domains)
  database/           # DynamoDB connection manager
tests/
  unit/
    messaging/        # Handler unit tests
    services/         # Service unit tests
  integration/        # Full-stack tests (moto)
terraform/resources/  # Infrastructure (per-environment)
```

## Current Domains

### teams-static

|                    | Nonlive                                           | Live                                           |
| ------------------ | ------------------------------------------------- | ---------------------------------------------- |
| **Lambda**         | `bball-app-data-consumption-teams-static-nonlive` | `bball-app-data-consumption-teams-static-live` |
| **DynamoDB table** | `bball-app-data-consumption-teams-static-nonlive` | `bball-app-data-consumption-teams-static-live` |

**Flow:**

1. Lambda is invoked
2. Handler fetches the latest raw document from `s3://{bucket}/raw/teams_static/` (sorted by `LastModified`, paginated)
3. Document is validated against `teams-static-raw-schema.json` (jsonschema)
4. Each team in the `payload` array is mapped to an `NbaTeam` model
5. Teams are batch-written to the DynamoDB table

**Handler:** `src.messaging.teams_static_handler.lambda_handler`

### games

|                    | Nonlive                                    | Live                                    |
| ------------------ | ------------------------------------------ | --------------------------------------- |
| **Lambda**         | `bball-app-data-consumption-games-nonlive` | `bball-app-data-consumption-games-live` |
| **DynamoDB table** | `bball-app-data-consumption-games-nonlive` | `bball-app-data-consumption-games-live` |

**Default scheduler behavior (no custom input):**

1. Fetch latest raw document from `s3://{bucket}/raw/schedule_league_v2/`
2. Validate against `games-raw-schema.json`
3. Map only regular-season NBA-vs-NBA games
4. Select candidates from **today 00:00 UTC** through **today + 14 days** (configurable with `GAMES_REFRESH_DAYS`)
5. Exclude FINAL games by default
6. Upsert only changed items (hash-based write skip)

#### Manual Input Options (for ad-hoc invocation)

Provide options under `event.input` when invoking manually.

| Option                         | Type    | Default                                   | Description                                                                      |
| ------------------------------ | ------- | ----------------------------------------- | -------------------------------------------------------------------------------- |
| `write_all_season_games`       | boolean | `false`                                   | When `true`, process all mapped regular-season NBA-vs-NBA games (backfill mode). |
| `from_date_utc`                | string  | `null`                                    | ISO datetime lower bound (inclusive) for replay mode.                            |
| `to_date_utc`                  | string  | `null`                                    | ISO datetime upper bound (inclusive) for replay mode. Requires `from_date_utc`.  |
| `replay_until_default_horizon` | boolean | `false`                                   | In replay mode, cap replay upper bound at today + `refresh_days`.                |
| `include_final_games`          | boolean | `null`                                    | Include FINAL games in candidate selection.                                      |
| `refresh_days`                 | integer | env (`GAMES_REFRESH_DAYS`, fallback `14`) | Horizon days used by default mode and replay-until-default-horizon mode.         |

#### Validation Rules

1. Unknown keys inside `event.input` are rejected.
2. `write_all_season_games` cannot be combined with replay range options.
3. `to_date_utc` requires `from_date_utc`.
4. `to_date_utc` and `replay_until_default_horizon` are mutually exclusive.
5. `to_date_utc` must be greater than or equal to `from_date_utc`.
6. `refresh_days` must be an integer >= 1.

#### Examples

Backfill full season:

```json
{
  "input": {
    "write_all_season_games": true,
    "include_final_games": true
  }
}
```

Replay from a date to a date:

```json
{
  "input": {
    "from_date_utc": "2026-03-01T00:00:00Z",
    "to_date_utc": "2026-03-10T23:59:59Z",
    "include_final_games": true
  }
}
```

Replay from a date until default horizon:

```json
{
  "input": {
    "from_date_utc": "2026-03-01T00:00:00Z",
    "replay_until_default_horizon": true,
    "include_final_games": true
  }
}
```

## Monitoring (live only)

- **CloudWatch Alarm** on Lambda `Errors` metric — triggers if errors > 0 over two consecutive 5-minute periods
- **SNS Topic** (`bball-app-live-data-consumption-alarms`) sends email notifications to configured addresses
- Alarm emails are configured via the `alarm_emails` Terraform variable

## Adding a New Domain

1. Create a model in `src/model/`
2. Create a repository in `src/repository/`
3. Create a service in `src/service/`
4. Create a handler in `src/messaging/`
5. Add a raw schema in `src/messaging/schemas/`
6. Add Terraform resources (Lambda, DynamoDB table, IAM policies)
7. Add tests under `tests/unit/messaging/`, `tests/unit/services/`, and `tests/integration/`

## Running Locally

```bash
# Install dependencies
pip install -r requirements.txt -r requirements-dev.txt

# Unit tests
poe test

# Integration tests (uses moto)
poe test-integration

# All tests
poe test-all
```
