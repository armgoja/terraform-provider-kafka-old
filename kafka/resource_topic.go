package kafka

import (
	"errors"
	"fmt"
	"log"

	"github.com/Shopify/sarama"
	"github.com/hashicorp/terraform/helper/schema"
	"github.com/armgoja/terraform-provider-kafka-old/kafka/helper"
)

var r = helper.ResourceHelper{}

func resourceKafkaTopic() *schema.Resource {
	return &schema.Resource{
		Create: resourceKafkaTopicCreate,
		Read:   resourceKafkaTopicRead,
		Update: resourceKafkaTopicUpdate,
		Delete: resourceKafkaTopicDelete,
		Importer: &schema.ResourceImporter{
			State: schema.ImportStatePassthrough,
		},
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
		"config_entries": &schema.Schema{
			Type:     schema.TypeMap,
			Optional: true,
			Computed: true,
		},
	}
}

func resourceKafkaTopicCreate(d *schema.ResourceData, m interface{}) error {
	broker := m.(*sarama.Broker)
	defer broker.Close()

	// Get basic topic properties from input
	topic, err := r.CreateResourceParams(d)

	// Prepare CreateTopicRequest
	topicRequest := r.CreateKafkaTopicRequest(
		topic.Name,
		topic.Partitions,
		topic.ReplicationFactor,
		topic.ConfigEntries,
	)

	// Create a kafka topic using broker
	response, err := broker.CreateTopics(topicRequest)
	if err != nil {
		log.Printf("Error creating kafka Topic :: %s", err.Error())
		return err
	}
	// check and send error if any
	if response.TopicErrors[topic.Name].Err != sarama.ErrNoError {
		return fmt.Errorf("topic error: %v", response.TopicErrors[topic.Name].Err)
	}
	d.SetId(topic.Name)
	return resourceKafkaTopicRead(d, m)
}

func resourceKafkaTopicRead(d *schema.ResourceData, m interface{}) error {
	broker := m.(*sarama.Broker)
	defer broker.Close()

	topic := d.Id()

	response, err := broker.GetMetadata(r.GetKafkaMetadataRequest(topic))

	if err != nil {
		log.Println(err.Error())
		return err
	}

	log.Printf("[DEBUG] Kafka: Topic retrieved for %s: %#v", topic, response)

	if len(response.Topics) != 1 {
		msg := "The requested topic does not exist"
		log.Println(msg)
		return errors.New(msg)
	}

	var partitions int
	var replicationFactor int

	for _, topic := range response.Topics {
		partitions = len(topic.Partitions)
		for _, partition := range topic.Partitions {
			replicationFactor = len(partition.Replicas)
			continue
		}
	}

	d.Set("name", topic)
	d.Set("partitions", partitions)
	d.Set("replication_factor", replicationFactor)

	configs, err := broker.DescribeConfigs(r.GetKafkaConfigsRequest(topic, r.ValidConfigNames()))

	if err != nil {
		log.Println(err.Error())
		return err
	}

	if len(configs.Resources) != 1 {
		msg := "The requested topic configs does not exist"
		log.Println(msg)
		return errors.New(msg)
	}

	configEntries := make(map[string]interface{})

	for _, resource := range configs.Resources {
		for _, config := range resource.Configs {
			configEntries[config.Name] = config.Value
		}
	}

	log.Printf("[DEBUG] configs: %#v", configEntries)

	d.Set("config_entries", configEntries)

	log.Printf("[DEBUG] resource data: %#v", d.Get("config_entries"))

	return nil
}

func resourceKafkaTopicUpdate(d *schema.ResourceData, m interface{}) error {
	broker := m.(*sarama.Broker)
	defer broker.Close()

	topic := d.Get("name").(string)

	d.Partial(true)

	// Replication factor cannot be changed
	if d.HasChange("replication_factor") {
		msg := "Replication factor cannot be changed on the fly"
		log.Println(msg)
		return errors.New(msg)
	}

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

	if d.HasChange("config_entries") {
		_, newVal := d.GetChange("config_entries")
		request := r.AlterTopicConfigsRequest(topic, newVal.(map[string]interface{}))
		response, err := broker.AlterConfigs(request)
		if err != nil {
			log.Println(err.Error())
			return err
		}

		for _, r := range response.Resources {
			log.Printf("[DEBUG] alter response: %#v", r)
			if r.ErrorCode != 0 {
				err = errors.New(r.ErrorMsg)
				return err
			}
		}
	}

	d.SetId(topic)

	d.Partial(false)

	return resourceKafkaTopicRead(d, m)
}

func resourceKafkaTopicDelete(d *schema.ResourceData, m interface{}) error {
	broker := m.(*sarama.Broker)
	defer broker.Close()

	topic := d.Id()

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
