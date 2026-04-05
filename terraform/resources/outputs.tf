output "lambda_function_name" {
  description = "Name of the Lambda function"
  value       = aws_lambda_function.function.function_name
}

output "lambda_function_arn" {
  description = "ARN of the Lambda function"
  value       = aws_lambda_function.function.arn
}

output "lambda_role_arn" {
  description = "ARN of the Lambda execution role"
  value       = aws_iam_role.lambda_role.arn
}

output "lambda_log_group" {
  description = "CloudWatch log group for the Lambda function"
  value       = aws_cloudwatch_log_group.lambda_logs.name
}

output "lambda_invoke_arn" {
  description = "Invoke ARN of the Lambda function"
  value       = aws_lambda_function.function.invoke_arn
}

output "dynamodb_table_name" {
  description = "Name of the teams-static DynamoDB table"
  value       = aws_dynamodb_table.teams_static.name
}

output "dynamodb_table_arn" {
  description = "ARN of the teams-static DynamoDB table"
  value       = aws_dynamodb_table.teams_static.arn
}

output "lambda_deadletter_queue_url" {
  description = "SQS DLQ URL for failed Lambda invocations"
  value       = var.environment == "live" ? aws_sqs_queue.lambda_deadletter[0].url : null
}

output "lambda_deadletter_queue_arn" {
  description = "SQS DLQ ARN for failed Lambda invocations"
  value       = var.environment == "live" ? aws_sqs_queue.lambda_deadletter[0].arn : null
}
