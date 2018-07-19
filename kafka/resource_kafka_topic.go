package kafka

import (
	"context"
	"log"

	"github.com/hashicorp/terraform/helper/schema"
	"github.com/segmentio/kafka-go"
)

func dialConnection(topic string, partition int) (*kafka.Conn, error) {
	var conn *kafka.Conn
	var err error
	if conn, err = kafka.DialLeader(context.Background(), "tcp", "localhost:9092", topic, partition); err != nil {
		return nil, err
	}
	return conn, nil
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
			Optional:    true,
			Description: "Replication factor for this Topic",
		},
	}
}

func resourceKafkaTopicCreate(d *schema.ResourceData, m interface{}) error {
	// Get basic topic properties from input
	name, partition, replicationFactor, err := extractCreateParams(d)
	log.Printf("Topic Name : %s , Number of Partitions : %d , Replication Factor : %d", name, partition, replicationFactor)

	// Create a kafka admin client
	conn, err := dialConnection(name, partition)
	if err != nil {
		log.Print("Error creating kafka connection", err.Error())
		return err
	}
	defer conn.Close()

	// Kafka topic configuration
	topicConfig := createTopicConfig(name, partition, replicationFactor)
	log.Printf("Kafka Topic Configuration :: %#v", topicConfig)

	// Create a kafka topic using client
	if err = conn.CreateTopics(topicConfig); err != nil {
		log.Printf("Error creating kafka Topic :: %s", err.Error())
		return err
	}

	d.SetId(name)

	// check and send error if any
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

func createTopicConfig(name string, partition int, replicationFactor int) kafka.TopicConfig {
	return kafka.TopicConfig{
		NumPartitions:     partition,
		Topic:             name,
		ReplicationFactor: replicationFactor,
	}
}
