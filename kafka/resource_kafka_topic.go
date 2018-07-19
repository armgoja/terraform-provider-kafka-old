package kafka

import (
	"errors"
	"fmt"
	"log"
	"time"

	"github.com/Shopify/sarama"
	"github.com/hashicorp/terraform/helper/schema"
)

func brokerConnection() (*sarama.Broker, error) {
	// new broker instance
	broker := sarama.NewBroker("localhost:9092")

	// broker configuration
	config := sarama.NewConfig()
	config.Version = sarama.V1_0_0_0

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

func resourceKafkaTopic() *schema.Resource {
	return &schema.Resource{
		Create: resourceKafkaTopicCreate,
		Read:   resourceKafkaTopicRead,
		Update: resourceKafkaTopicUpdate,
		Delete: resourceKafkaTopicDelete,
		Schema: kafkaSchema(),
	}
}

func kafkaSchema() map[string]*schema.Schema {
	return map[string]*schema.Schema{
		"name": &schema.Schema{
			Type:        schema.TypeString,
			Required:    true,
			Description: "Name of topic",
		},
		"partitions": &schema.Schema{
			Type:        schema.TypeInt,
			Required:    true,
			Description: "Number of partitions for this Topic",
		},
		"replication_factor": &schema.Schema{
			Type:        schema.TypeInt,
			Required:    true,
			Description: "Replication factor for this Topic",
		},
	}
}

func resourceKafkaTopicCreate(d *schema.ResourceData, m interface{}) error {
	var broker *sarama.Broker
	var err error
	// Get basic topic properties from input
	name, partition, replicationFactor, err := extractCreateParams(d)

	// Get the broker instance
	if broker, err = brokerConnection(); err != nil {
		log.Println("Broker connection failed")
		return err
	}
	defer broker.Close()

	// Prepare CreateTopicRequest
	topicRequest := createTopicRequest(name, partition, replicationFactor)

	// Create a kafka topic using broker
	response, err := broker.CreateTopics(topicRequest)
	if err != nil {
		log.Printf("Error creating kafka Topic :: %s", err.Error())
		return err
	}
	// check and send error if any
	if response.TopicErrors[name].Err != sarama.ErrNoError {
		return fmt.Errorf("topic error: %v", response.TopicErrors[name].Err)
	}
	d.SetId(name)
	return nil
}

func resourceKafkaTopicRead(d *schema.ResourceData, m interface{}) error {
	return nil
}

func resourceKafkaTopicUpdate(d *schema.ResourceData, m interface{}) error {
	return nil
}

func resourceKafkaTopicDelete(d *schema.ResourceData, m interface{}) error {
	return nil
}

/////////////// Helper methods. These can be extracted to another file later

func extractCreateParams(d *schema.ResourceData) (string, int, int, error) {
	name := d.Get("name").(string)
	partition := d.Get("partitions").(int)
	replicationFactor := d.Get("replication_factor").(int)
	log.Printf("Topic Name : %s , Number of Partitions : %d , Replication Factor : %d", name, partition, replicationFactor)

	// TBD : Perform some validations if required
	return name, partition, replicationFactor, nil
}

func createTopicRequest(name string, partition int, replicationFactor int) *sarama.CreateTopicsRequest {
	topicDetail := &sarama.TopicDetail{}
	topicDetail.NumPartitions = int32(partition)
	if replicationFactor != 0 {
		topicDetail.ReplicationFactor = int16(replicationFactor)
	}
	// Can add other configurations here if required
	topicDetail.ConfigEntries = make(map[string]*string)

	topicDetails := make(map[string]*sarama.TopicDetail)
	topicDetails[name] = topicDetail

	return &sarama.CreateTopicsRequest{
		Timeout:      time.Second * 15,
		TopicDetails: topicDetails,
	}
}
