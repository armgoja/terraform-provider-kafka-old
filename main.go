package main

import (
  "github.com/hashicorp/terraform/plugin"
  "github.com/sysco-middleware/terraform-provider-kafka/kafka"
)

func main() {
  plugin.Serve(&plugin.ServeOpts{
    ProviderFunc: kafka.Provider})
}
