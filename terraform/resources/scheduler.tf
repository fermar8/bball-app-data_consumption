# Daily scheduler for games Lambda at 13:00 UTC.
# Disabled by default and enabled only when explicitly requested.

resource "aws_cloudwatch_event_rule" "games_daily" {
  name                = "${var.games_function_name}-${var.environment}-daily"
  description         = "Daily trigger for games data consumption"
  schedule_expression = "cron(0 13 * * ? *)"
  state               = var.games_scheduler_enabled ? "ENABLED" : "DISABLED"

  tags = merge(var.tags, { Environment = var.environment })
}

resource "aws_cloudwatch_event_target" "games_daily_lambda" {
  rule      = aws_cloudwatch_event_rule.games_daily.name
  target_id = "${var.games_function_name}-lambda"
  arn       = aws_lambda_function.games_function.arn
}

resource "aws_lambda_permission" "allow_events_invoke_games" {
  statement_id  = "AllowExecutionFromCloudWatchEvents"
  action        = "lambda:InvokeFunction"
  function_name = aws_lambda_function.games_function.function_name
  principal     = "events.amazonaws.com"
  source_arn    = aws_cloudwatch_event_rule.games_daily.arn
}
