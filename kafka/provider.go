package kafka

import (
	"github.com/hashicorp/terraform/helper/schema"
	"github.com/hashicorp/terraform/terraform"
)

// Provider implements terraform.ResourceProvider.
// It exposes all the fields required to create a kafka provider
func Provider() terraform.ResourceProvider {

	// mapping of resources against the operations that are available on the resource
	resourceMap := map[string]*schema.Resource{
		"kafka_topic": resourceKafkaTopic(),
	}

	// return the Provider with map of resources and their operations
	return &schema.Provider{ResourcesMap: resourceMap}
}
