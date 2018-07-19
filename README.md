# terraform-provider-kafka

## Steps to test
- From the application root run `go build -o terraform-provider-kafka`. This will create a binary under project root.
- If the build fails because of certail dependencies, you need to pull them manually.
- Set kafka topic configurations in mail.tf
- Run these commands in sequence `terraform init`, `terraform plan`, `terraform apply`
- To see the logs and messages set environment variable `TF_LOG=TRACE`


## Requirements
- Install Go and set it to PATH
- Download terraform and set it to PATH


## Configurations
- Broker configurations are hardcoded