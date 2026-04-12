# Data source to reference bootstrap infrastructure
data "terraform_remote_state" "bootstrap" {
  backend = "s3"

  config = {
    bucket = var.bootstrap_state_bucket
    key    = "bootstrap/roles-and-db-config.tfstate"
    region = var.aws_region
  }
}
