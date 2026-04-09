# Lambda package built from the src/ directory
data "archive_file" "lambda_zip" {
  type        = "zip"
  source_dir  = "${path.module}/../../src"
  output_path = "${path.module}/lambda.zip"
}

# IAM role for Lambda execution
resource "aws_iam_role" "lambda_role" {
  name = "${var.function_name}-${var.environment}-role"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Action = "sts:AssumeRole"
        Effect = "Allow"
        Principal = {
          Service = "lambda.amazonaws.com"
        }
      }
    ]
  })

  tags = merge(var.tags, { Environment = var.environment })
}

# Attach basic Lambda execution policy (CloudWatch Logs)
resource "aws_iam_role_policy_attachment" "lambda_basic" {
  policy_arn = "arn:aws:iam::aws:policy/service-role/AWSLambdaBasicExecutionRole"
  role       = aws_iam_role.lambda_role.name
}

# CloudWatch Log Group
resource "aws_cloudwatch_log_group" "lambda_logs" {
  name              = "/aws/lambda/${var.function_name}-${var.environment}"
  retention_in_days = var.log_retention_days

  tags = merge(var.tags, { Environment = var.environment })
}

# IAM policy: allow Lambda to read from the NBA data S3 bucket
resource "aws_iam_policy" "lambda_s3_read" {
  name        = "${var.function_name}-${var.environment}-s3-read-policy"
  description = "Allow Lambda to read raw NBA data from S3"

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Action = [
          "s3:GetObject",
          "s3:ListBucket",
        ]
        Resource = [
          "arn:aws:s3:::${var.nba_data_bucket_name}",
          "arn:aws:s3:::${var.nba_data_bucket_name}/*",
        ]
      }
    ]
  })

  tags = merge(var.tags, { Environment = var.environment })
}

resource "aws_iam_role_policy_attachment" "lambda_s3_read" {
  role       = aws_iam_role.lambda_role.name
  policy_arn = aws_iam_policy.lambda_s3_read.arn
}

# IAM policy: allow Lambda to read/write the teams-static DynamoDB table
resource "aws_iam_policy" "lambda_dynamodb" {
  name        = "${var.function_name}-${var.environment}-dynamodb-policy"
  description = "Allow Lambda to access the teams-static DynamoDB table"

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Action = [
          "dynamodb:GetItem",
          "dynamodb:PutItem",
          "dynamodb:UpdateItem",
          "dynamodb:DeleteItem",
          "dynamodb:Query",
          "dynamodb:Scan",
          "dynamodb:BatchWriteItem",
        ]
        Resource = [
          aws_dynamodb_table.teams_static.arn,
          "${aws_dynamodb_table.teams_static.arn}/index/*",
        ]
      }
    ]
  })

  tags = merge(var.tags, { Environment = var.environment })
}

resource "aws_iam_role_policy_attachment" "lambda_dynamodb" {
  role       = aws_iam_role.lambda_role.name
  policy_arn = aws_iam_policy.lambda_dynamodb.arn
}

# Lambda Function
resource "aws_lambda_function" "function" {
  filename         = data.archive_file.lambda_zip.output_path
  function_name    = "${var.function_name}-${var.environment}"
  role             = aws_iam_role.lambda_role.arn
  handler          = "src.messaging.teams_static_handler.lambda_handler"
  source_code_hash = data.archive_file.lambda_zip.output_base64sha256
  runtime          = "python3.12"
  timeout          = var.timeout
  memory_size      = var.memory_size

  environment {
    variables = {
      ENVIRONMENT         = var.environment
      DYNAMODB_TABLE_NAME = aws_dynamodb_table.teams_static.name
      S3_BUCKET_NAME      = var.nba_data_bucket_name
    }
  }

  tags = merge(var.tags, { Environment = var.environment })

  depends_on = [
    aws_cloudwatch_log_group.lambda_logs,
    aws_iam_role_policy_attachment.lambda_basic,
    aws_iam_role_policy_attachment.lambda_dynamodb,
    aws_iam_role_policy_attachment.lambda_s3_read,
  ]
}
