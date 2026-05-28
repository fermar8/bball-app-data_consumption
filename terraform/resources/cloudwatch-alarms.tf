# SNS Topic for CloudWatch Alarms (live only)
resource "aws_sns_topic" "lambda_alarms" {
  count = var.environment == "live" ? 1 : 0
  name  = "${var.project_name}-${var.environment}-data-consumption-alarms"

  tags = merge(var.tags, {
    Environment = var.environment
    Purpose     = "Lambda error notifications"
  })
}

# CloudWatch Alarm - Lambda Errors (live only)
resource "aws_cloudwatch_metric_alarm" "lambda_errors" {
  count               = var.environment == "live" ? 1 : 0
  alarm_name          = "${var.teams_static_function_name}-${var.environment}-errors"
  comparison_operator = "GreaterThanThreshold"
  evaluation_periods  = 2
  metric_name         = "Errors"
  namespace           = "AWS/Lambda"
  period              = 300
  statistic           = "Sum"
  threshold           = 0
  alarm_description   = "Triggers when the teams-static Lambda has sustained errors"
  treat_missing_data  = "notBreaching"

  dimensions = {
    FunctionName = aws_lambda_function.function.function_name
  }

  alarm_actions = [aws_sns_topic.lambda_alarms[0].arn]
  ok_actions    = [aws_sns_topic.lambda_alarms[0].arn]

  tags = merge(var.tags, { Environment = var.environment })
}

# CloudWatch Alarm - Games Lambda Errors (live only)
resource "aws_cloudwatch_metric_alarm" "games_lambda_errors" {
  count               = var.environment == "live" ? 1 : 0
  alarm_name          = "${var.games_function_name}-${var.environment}-errors"
  comparison_operator = "GreaterThanThreshold"
  evaluation_periods  = 2
  metric_name         = "Errors"
  namespace           = "AWS/Lambda"
  period              = 300
  statistic           = "Sum"
  threshold           = 0
  alarm_description   = "Triggers when the games Lambda has sustained errors"
  treat_missing_data  = "notBreaching"

  dimensions = {
    FunctionName = aws_lambda_function.games_function.function_name
  }

  alarm_actions = [aws_sns_topic.lambda_alarms[0].arn]
  ok_actions    = [aws_sns_topic.lambda_alarms[0].arn]

  tags = merge(var.tags, { Environment = var.environment })
}

# CloudWatch Alarm - Players Index Lambda Errors (live only)
resource "aws_cloudwatch_metric_alarm" "players_index_lambda_errors" {
  count               = var.environment == "live" ? 1 : 0
  alarm_name          = "${var.players_index_function_name}-${var.environment}-errors"
  comparison_operator = "GreaterThanThreshold"
  evaluation_periods  = 2
  metric_name         = "Errors"
  namespace           = "AWS/Lambda"
  period              = 300
  statistic           = "Sum"
  threshold           = 0
  alarm_description   = "Triggers when the players_index Lambda has sustained errors"
  treat_missing_data  = "notBreaching"

  dimensions = {
    FunctionName = aws_lambda_function.players_index_function.function_name
  }

  alarm_actions = [aws_sns_topic.lambda_alarms[0].arn]
  ok_actions    = [aws_sns_topic.lambda_alarms[0].arn]

  tags = merge(var.tags, { Environment = var.environment })
}

# Email subscriptions for alarms
resource "aws_sns_topic_subscription" "lambda_alarms_email" {
  for_each  = var.environment == "live" ? toset(var.alarm_emails) : []
  topic_arn = aws_sns_topic.lambda_alarms[0].arn
  protocol  = "email"
  endpoint  = each.value
}

output "lambda_alarms_topic_arn" {
  description = "SNS topic ARN for Lambda alarms"
  value       = var.environment == "live" ? aws_sns_topic.lambda_alarms[0].arn : null
}
