package kafka

import (
	"errors"
	"fmt"
	"log"

	"github.com/Shopify/sarama"
	"github.com/hashicorp/terraform/helper/schema"
	"github.com/sysco-middleware/terraform-provider-kafka/kafka/helper"
)

var r = helper.ResourceHelper{}

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
	name, partition, replicationFactor, err := r.CreateResourceParams(d)

	// Get the broker instance
	if broker, err = brokerConnection(); err != nil {
		log.Println("Broker connection failed")
		return err
	}
	defer broker.Close()

	// Prepare CreateTopicRequest
	topicRequest := r.CreateKafkaTopicRequest(name, partition, replicationFactor)

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
	topic := d.Get("name").(string)
	broker, err := brokerConnection()
	if err != nil {
		log.Println(err.Error())
		return err
	}
	defer broker.Close()

	response, err := broker.GetMetadata(r.GetKafkaMetadataRequest(topic))

	if err != nil {
		log.Println(err.Error())
		return err
	}

	if len(response.Topics) < 1 {
		msg := "The requested topic does not exist"
		log.Println(msg)
		return errors.New(msg)
	}
	return nil
}

func resourceKafkaTopicUpdate(d *schema.ResourceData, m interface{}) error {

	topic := d.Get("name").(string)

	// Replication factor cannot be changed
	if d.HasChange("replication_factor") {
		msg := "Replication factor cannot be changed on the fly"
		log.Println(msg)
		return errors.New(msg)
	}

	broker, err := brokerConnection()
	if err != nil {
		log.Println(err.Error())
		return err
	}
	defer broker.Close()

	// Check if there is change in number of partitions
	if d.HasChange("partitions") {
		oldVal, newVal := d.GetChange("partitions")
		// Validate the number of partitions
		if newVal.(int) < oldVal.(int) {
			msg := fmt.Sprintf("Number of partitions can not be reduced, please provide a value greater than %s", oldVal.(string))
			log.Println(msg)
			return errors.New(msg)
		}
		request := r.CreateKafkaPartitionRequest(topic, newVal.(int32))
		response, err := broker.CreatePartitions(request)
		if err != nil {
			log.Println(err.Error())
			return err
		}

		if err := response.TopicPartitionErrors[topic]; err.Err != sarama.ErrNoError {
			log.Println(err.Err.Error())
			return err.Err
		}
	}
	return nil
}

func resourceKafkaTopicDelete(d *schema.ResourceData, m interface{}) error {

	broker, err := brokerConnection()
	if err != nil {
		return err
	}
	defer broker.Close()

	topic := d.Get("name").(string)

	if topic == "" || &topic == nil {
		return errors.New("Provide a topic name")
	}

	response, err := broker.DeleteTopics(r.DeleteKafkaTopicRequest(topic))

	if err != nil {
		log.Printf("Error Deleting topic %s", err.Error())
		return err
	}

	if response.TopicErrorCodes[topic] != sarama.ErrNoError {
		return errors.New(response.TopicErrorCodes[topic].Error())
	}
	return nil
}
