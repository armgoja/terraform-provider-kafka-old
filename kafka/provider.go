package kafka

import (
	"errors"
	"fmt"
	"log"

	"github.com/Shopify/sarama"
	"github.com/hashicorp/terraform/helper/schema"
	"github.com/hashicorp/terraform/terraform"
)

// Provider implements terraform.ResourceProvider.
// It exposes all the fields required to create a kafka provider
func Provider() terraform.ResourceProvider {

	// return the Provider with map of resources and their operations
	return &schema.Provider{
		Schema: map[string]*schema.Schema{
			"broker_list": &schema.Schema{
				Type:        schema.TypeString,
				Required:    true,
				DefaultFunc: schema.EnvDefaultFunc("KAFKA_BROKER_LIST", nil),
				ValidateFunc: func(v interface{}, k string) (ws []string, errors []error) {
					value := v.(string)
					if value == "" {
						errors = append(errors, fmt.Errorf("Broker list must not be an empty string"))
					}

					return
				},
			},
		},

		ResourcesMap: map[string]*schema.Resource{
			"kafka_topic": resourceKafkaTopic(),
		},

		ConfigureFunc: provideConfigure,
	}

}

func provideConfigure(d *schema.ResourceData) (interface{}, error) {
	brokerList := d.Get("broker_list").(string)

	broker, err := brokerConnection(brokerList)

	if err != nil {
		return nil, err
	}

	return broker, nil
}

func brokerConnection(brokerList string) (*sarama.Broker, error) {
	// new broker instance
	broker := sarama.NewBroker(brokerList)

	// broker configuration
	config := sarama.NewConfig()
	config.Version = sarama.V0_11_0_2

	// open broker with defined broker configuration
	err := broker.Open(config)
	if err != nil {
		log.Println("Error establishing connection to broker ", err.Error())
		return nil, err
	}

	// check if broker connection is available
	connected, err := broker.Connected()
	if err != nil {
		log.Println("Broker not connected ", err.Error())
		return nil, err
	} else if connected != true {
		log.Println("Broker is not connected")
		return nil, errors.New("Broker not connected")
	}

	return broker, nil
}
