package main

import (
	"github.com/hashicorp/terraform/plugin"
	"github.com/sysco-middleware/terraform-provider-kafka/kafka"
)

// This is the entry point for this provider.
// We start by defining the options that will be served by this plugin.
// Each option returns its own set of resources and operations available against the resource.
func main() {
	opts := plugin.ServeOpts{ProviderFunc: kafka.Provider}
	plugin.Serve(&opts)
}
