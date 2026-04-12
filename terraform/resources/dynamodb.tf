# DynamoDB table for teams-static data
# Created per environment: bball-app-data-consumption-teams-static-nonlive / bball-app-data-consumption-teams-static-live

resource "aws_dynamodb_table" "teams_static" {
  name         = "bball-app-data-consumption-teams-static-${var.environment}"
  billing_mode = "PAY_PER_REQUEST"
  hash_key     = "teamId"

  attribute {
    name = "teamId"
    type = "N"
  }

  tags = merge(
    var.tags,
    {
      Name        = "bball-app-data-consumption-teams-static-${var.environment}"
      Environment = var.environment
    }
  )
}
