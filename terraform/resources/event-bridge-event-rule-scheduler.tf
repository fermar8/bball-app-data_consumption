# EventBridge rule to trigger the Lambda on a schedule
resource "aws_cloudwatch_event_rule" "lambda_schedule" {
  name                = "${var.function_name}-schedule-${var.environment}"
  description         = "Trigger data-consumption-teams-static Lambda on a schedule"
  schedule_expression = var.scheduler_expression
  state               = var.scheduler_enabled ? "ENABLED" : "DISABLED"

  tags = merge(var.tags, { Environment = var.environment })
}

# Target: connect the rule to the Lambda function
resource "aws_cloudwatch_event_target" "lambda_target" {
  rule      = aws_cloudwatch_event_rule.lambda_schedule.name
  target_id = "LambdaTarget"
  arn       = aws_lambda_function.function.arn

  dynamic "retry_policy" {
    for_each = var.environment == "live" ? [1] : []
    content {
      maximum_event_age_in_seconds = 60
      maximum_retry_attempts       = 0
    }
  }

  input = jsonencode({
    action = "consume_teams_static"
  })
}

# Allow EventBridge to invoke the Lambda
resource "aws_lambda_permission" "allow_eventbridge" {
  statement_id  = "AllowExecutionFromEventBridge"
  action        = "lambda:InvokeFunction"
  function_name = aws_lambda_function.function.function_name
  principal     = "events.amazonaws.com"
  source_arn    = aws_cloudwatch_event_rule.lambda_schedule.arn
}
