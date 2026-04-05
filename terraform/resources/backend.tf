terraform {
  backend "s3" {
    # Override at runtime with -backend-config=backend.hcl
    bucket         = "tfstate-590183661886-eu-west-3"
    key            = "resources/nonlive/bball-app-data-consumption.tfstate"
    region         = "eu-west-3"
    dynamodb_table = "terraform-state-lock"
    encrypt        = true
  }
}
