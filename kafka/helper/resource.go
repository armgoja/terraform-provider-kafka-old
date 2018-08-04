package helper

import (
	"errors"
	"log"
	"time"

	"github.com/Shopify/sarama"
	"github.com/hashicorp/terraform/helper/schema"
)

// ResourceHelper exposes certain utility methods to configure sarama requests
type ResourceHelper struct{}

// CreateResourceParams parses the required kafka inputs
func (*ResourceHelper) CreateResourceParams(d *schema.ResourceData) (string, int, int, error) {
	if d == nil {
		return "", 0, 0, errors.New("Invalid input")
	}
	name := d.Get("name").(string)
	partition := d.Get("partitions").(int)
	replicationFactor := d.Get("replication_factor").(int)
	log.Printf("Topic Name : %s , Number of Partitions : %d , Replication Factor : %d", name, partition, replicationFactor)

	// TBD : Perform some validations if required
	return name, partition, replicationFactor, nil
}

// CreateKafkaTopicRequest prepares sarama.CreateTopicsRequest from arguments
func (*ResourceHelper) CreateKafkaTopicRequest(name string, partition int, replicationFactor int) *sarama.CreateTopicsRequest {
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

// DeleteKafkaTopicRequest prepares sarama.DeleteTopicsRequest from arguments
func (*ResourceHelper) DeleteKafkaTopicRequest(topic string) *sarama.DeleteTopicsRequest {
	return &sarama.DeleteTopicsRequest{
		Topics:  []string{topic},
		Timeout: time.Second * 20,
	}
}

// CreateKafkaPartitionRequest prepares sarama.CreatePartitionsRequest from arguments
func (*ResourceHelper) CreateKafkaPartitionRequest(topic string, partitions int32) *sarama.CreatePartitionsRequest {
	var topicPartition sarama.TopicPartition
	topicPartition.Count = partitions

	var request sarama.CreatePartitionsRequest
	request.Timeout = time.Second * 15
	request.TopicPartitions = map[string]*sarama.TopicPartition{topic: &topicPartition}

	return &request
}

// GetKafkaMetadataRequest prepares sarama.MetadataRequest from arguments
func (*ResourceHelper) GetKafkaMetadataRequest(topic string) *sarama.MetadataRequest {
	return &sarama.MetadataRequest{
		Topics: []string{topic},
	}
}
