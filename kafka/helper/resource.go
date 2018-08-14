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

type Topic struct {
	Name              string
	Partitions        int
	ReplicationFactor int
	ConfigEntries     map[string]*string
}

func (*ResourceHelper) ValidConfigNames() []string {
	return []string{
		"cleanup.policy",
		"compression.type",
		"delete.retention.ms",
		"file.delete.delay.ms",
		"flush.messages",
		"flush.ms",
		"follower.replication.throttled.replicas",
		"index.interval.bytes",
		"leader.replication.throttled.replicas",
		"max.message.bytes",
		"message.format.version",
		"message.timestamp.difference.max.ms",
		"message.timestamp.type",
		"min.cleanable.dirty.ratio",
		"min.insync.replicas",
		"preallocate",
		"retention.bytes",
		"retention.ms",
		"segment.bytes",
		"segment.index.bytes",
		"segment.jitter.ms",
		"segment.ms",
		"unclean.leader.election.enable",
		"message.downconversion.enable",
	}

}

// CreateResourceParams parses the required kafka inputs
func (*ResourceHelper) CreateResourceParams(d *schema.ResourceData) (Topic, error) {
	if d == nil {
		return Topic{}, errors.New("Invalid input")
	}
	name := d.Get("name").(string)
	partitions := d.Get("partitions").(int)
	replicationFactor := d.Get("replication_factor").(int)

	configEntries := make(map[string]*string)

	configs := d.Get("config_entries").(map[string]interface{})

	for config, entry := range configs {
		entryValue := entry.(string)
		configEntries[config] = &entryValue
	}

	topic := Topic{
		Name:              name,
		Partitions:        partitions,
		ReplicationFactor: replicationFactor,
		ConfigEntries:     configEntries,
	}
	log.Printf("Topic: %#v", topic)
	// TBD : Perform some validations if required
	return topic, nil
}

// CreateKafkaTopicRequest prepares sarama.CreateTopicsRequest from arguments
func (*ResourceHelper) CreateKafkaTopicRequest(
	name string,
	partitions int,
	replicationFactor int,
	configEntries map[string]*string,
) *sarama.CreateTopicsRequest {
	// TODO: define replica assignment on input schema
	var replicaAssignment map[int32][]int32
	topicDetail := &sarama.TopicDetail{
		NumPartitions:     int32(partitions),
		ReplicationFactor: int16(replicationFactor),
		ReplicaAssignment: replicaAssignment,
		ConfigEntries:     configEntries,
	}
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

func (*ResourceHelper) GetKafkaConfigsRequest(topic string, configNames []string) *sarama.DescribeConfigsRequest {
	return &sarama.DescribeConfigsRequest{
		Resources: []*sarama.ConfigResource{
			{
				Type:        sarama.TopicResource,
				Name:        topic,
				ConfigNames: configNames,
			},
		},
	}
}

func (*ResourceHelper) AlterTopicConfigsRequest(topic string, configs map[string]interface{}) *sarama.AlterConfigsRequest {
	configEntries := make(map[string]*string, len(configs))
	for config, entry := range configs {
		entryValue := entry.(string)
		configEntries[config] = &entryValue
	}
	return &sarama.AlterConfigsRequest{
		Resources: []*sarama.AlterConfigsResource{
			{
				Type:          sarama.TopicResource,
				Name:          topic,
				ConfigEntries: configEntries,
			},
		},
	}
}
