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

|                    | Nonlive                                 | Live                                 |
| ------------------ | --------------------------------------- | ------------------------------------ |
| **Lambda**         | `data-consumption-teams-static-nonlive` | `data-consumption-teams-static-live` |
| **DynamoDB table** | `teams-static-nonlive`                  | `teams-static-live`                  |

**Flow:**

1. Lambda is invoked
2. Handler fetches the latest raw document from `s3://{bucket}/raw/teams_static/` (sorted by `LastModified`, paginated)
3. Document is validated against `teams-static-raw-schema.json` (jsonschema)
4. Each team in the `payload` array is mapped to an `NbaTeam` model
5. Teams are batch-written to the DynamoDB table

**Handler:** `src.messaging.teams_static_handler.lambda_handler`

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
