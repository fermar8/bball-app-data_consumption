# DynamoDB table for teams-static data
# Created per environment: bball-app-data-consumption-teams-static-nonlive / bball-app-data-consumption-teams-static-live

resource "aws_dynamodb_table" "teams_static" {
  name         = "${var.teams_static_function_name}-${var.environment}"
  billing_mode = "PAY_PER_REQUEST"
  hash_key     = "teamId"

  attribute {
    name = "teamId"
    type = "N"
  }

  tags = merge(
    var.tags,
    {
      Name        = "${var.teams_static_function_name}-${var.environment}"
      Environment = var.environment
    }
  )
}

# DynamoDB table for games data
# Created per environment: bball-app-data-consumption-games-nonlive / bball-app-data-consumption-games-live

resource "aws_dynamodb_table" "games" {
  name         = "${var.games_function_name}-${var.environment}"
  billing_mode = "PAY_PER_REQUEST"
  hash_key     = "gameId"

  attribute {
    name = "gameId"
    type = "S"
  }

  attribute {
    name = "homeTeamId"
    type = "N"
  }

  attribute {
    name = "awayTeamId"
    type = "N"
  }

  attribute {
    name = "leagueKey"
    type = "S"
  }

  attribute {
    name = "gameDateTimeEst"
    type = "S"
  }

  global_secondary_index {
    name            = "byHomeTeamDateTime"
    hash_key        = "homeTeamId"
    range_key       = "gameDateTimeEst"
    projection_type = "ALL"
  }

  global_secondary_index {
    name            = "byAwayTeamDateTime"
    hash_key        = "awayTeamId"
    range_key       = "gameDateTimeEst"
    projection_type = "ALL"
  }

  global_secondary_index {
    name            = "byLeagueDateTime"
    hash_key        = "leagueKey"
    range_key       = "gameDateTimeEst"
    projection_type = "ALL"
  }

  tags = merge(
    var.tags,
    {
      Name        = "${var.games_function_name}-${var.environment}"
      Environment = var.environment
    }
  )
}
