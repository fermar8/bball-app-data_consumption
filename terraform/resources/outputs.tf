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

output "games_lambda_function_name" {
  description = "Name of the games Lambda function"
  value       = aws_lambda_function.games_function.function_name
}

output "games_lambda_function_arn" {
  description = "ARN of the games Lambda function"
  value       = aws_lambda_function.games_function.arn
}

output "games_dynamodb_table_name" {
  description = "Name of the games DynamoDB table"
  value       = aws_dynamodb_table.games.name
}

output "games_dynamodb_table_arn" {
  description = "ARN of the games DynamoDB table"
  value       = aws_dynamodb_table.games.arn
}

output "games_scheduler_rule_name" {
  description = "Name of the games scheduler rule"
  value       = aws_cloudwatch_event_rule.games_daily.name
}

output "players_index_lambda_function_name" {
  description = "Name of the players_index Lambda function"
  value       = aws_lambda_function.players_index_function.function_name
}

output "players_index_lambda_function_arn" {
  description = "ARN of the players_index Lambda function"
  value       = aws_lambda_function.players_index_function.arn
}

output "players_index_dynamodb_table_name" {
  description = "Name of the players_index DynamoDB table"
  value       = aws_dynamodb_table.players_index.name
}

output "players_index_dynamodb_table_arn" {
  description = "ARN of the players_index DynamoDB table"
  value       = aws_dynamodb_table.players_index.arn
}

output "players_index_scheduler_rule_name" {
  description = "Name of the players_index scheduler rule"
  value       = aws_cloudwatch_event_rule.players_index_weekly.name
}
