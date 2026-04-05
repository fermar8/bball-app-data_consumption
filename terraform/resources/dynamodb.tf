# DynamoDB table for teams-static data
# Created per environment: teams-static-nonlive / teams-static-live

resource "aws_dynamodb_table" "teams_static" {
  name         = "teams-static-${var.environment}"
  billing_mode = "PAY_PER_REQUEST"
  hash_key     = "teamId"

  attribute {
    name = "teamId"
    type = "N"
  }

  tags = merge(
    var.tags,
    {
      Name        = "teams-static-${var.environment}"
      Environment = var.environment
    }
  )
}
