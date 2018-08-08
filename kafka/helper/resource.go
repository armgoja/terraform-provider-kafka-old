package helper

import (
	"errors"
	"log"
	"time"
	"strconv"

	"github.com/Shopify/sarama"
	"github.com/hashicorp/terraform/helper/schema"
)

// ResourceHelper exposes certain utility methods to configure sarama requests
type ResourceHelper struct{}

// CreateResourceParams parses the required kafka inputs
func (*ResourceHelper) CreateResourceParams(d *schema.ResourceData) (string, int, int, map[string]*string, error) {
	configEntries := make(map[string]*string)
	if d == nil {
		return "", 0, 0, configEntries, errors.New("Invalid input")
	}
	name := d.Get("name").(string)
	partition := d.Get("partitions").(int)
	replicationFactor := d.Get("replication_factor").(int)
	log.Printf("Topic Name : %s , Number of Partitions : %d , Replication Factor : %d", name, partition, replicationFactor)
	cleanupPolicy := d.Get("cleanup_policy").(string)
	compressionType := d.Get("compression_type").(string)
	deleteRetentionMs := strconv.Itoa(d.Get("delete_retention_ms").(int))
	fileDeleteDelayMs := strconv.Itoa(d.Get("file_delete_delay_ms").(int))
	flushMessages := strconv.Itoa(d.Get("flush_messages").(int))
	flushMs := strconv.Itoa(d.Get("flush_ms").(int))
	followerReplicationThrottledReplicas := d.Get("follower_replication_throttled_replicas").(string)
	indexIntervalBytes := strconv.Itoa(d.Get("index_interval_bytes").(int))
	leaderReplicationThrottledReplicas := d.Get("leader_replication_throttled_replicas").(string)
	maxMessageBytes := strconv.Itoa(d.Get("max_message_bytes").(int))
	messageFormatVersion := d.Get("message_format_version").(string)
	messageTimestampDifferenceMaxMs := strconv.Itoa(d.Get("message_timestamp_difference_max_ms").(int))
	messageTimestampType := d.Get("message_timestamp_type").(string)
	minCleanableDirtyRatio := strconv.FormatFloat(d.Get("min_cleanable_dirty_ratio").(float64), 'E', -1, 64)
	minInsyncReplicas := strconv.Itoa(d.Get("min_insync_replicas").(int))
	preallocate := strconv.FormatBool(d.Get("preallocate").(bool))
	retentionBytes := strconv.Itoa(d.Get("retention_bytes").(int))
	retentionMs := strconv.Itoa(d.Get("retention_ms").(int))
	segmentBytes := strconv.Itoa(d.Get("segment_bytes").(int))
	segmentIndexBytes := strconv.Itoa(d.Get("segment_index_bytes").(int))
	segmentJitterMs := strconv.Itoa(d.Get("segment_jitter_ms").(int))
	segmentMs := strconv.Itoa(d.Get("segment_ms").(int))
	uncleanLeaderElectionEnable := strconv.FormatBool(d.Get("unclean_leader_election_enable").(bool))
	messageDownconversionEnable := strconv.FormatBool(d.Get("message_downconversion_enable").(bool))

	configEntries["cleanup.policy"] = &cleanupPolicy
	configEntries["compression.type"] = &compressionType
	configEntries["delete.retention.ms"] = &deleteRetentionMs
	configEntries["file.delete.delay.ms"] = &fileDeleteDelayMs
	configEntries["flush.messages"] = &flushMessages
	configEntries["flush.ms"] = &flushMs
	configEntries["follower.replication.throttled.replicas"] = &followerReplicationThrottledReplicas
	configEntries["index.interval.bytes"] = &indexIntervalBytes
	configEntries["leader.replication.throttled.replicas"] = &leaderReplicationThrottledReplicas
	configEntries["max.message.bytes"] = &maxMessageBytes
	configEntries["message.format.version"] = &messageFormatVersion
	configEntries["message.timestamp.difference.max.ms"] = &messageTimestampDifferenceMaxMs
	configEntries["message.timestamp.type"] = &messageTimestampType
	configEntries["min.cleanable.dirty.ratio"] = &minCleanableDirtyRatio
	configEntries["min.insync.replicas"] = &minInsyncReplicas
	configEntries["preallocate"] = &preallocate
	configEntries["retention.bytes"] = &retentionBytes
	configEntries["retention.ms"] = &retentionMs
	configEntries["segment.bytes"] = &segmentBytes
	configEntries["segment.index.bytes"] = &segmentIndexBytes
	configEntries["segment.jitter.ms"] = &segmentJitterMs
	configEntries["segment.ms"] = &segmentMs
	configEntries["unclean.leader.election.enable"] = &uncleanLeaderElectionEnable
	configEntries["message.downconversion.enable"] = &messageDownconversionEnable

	// TBD : Perform some validations if required
	return name, partition, replicationFactor, configEntries, nil
}

// CreateKafkaTopicRequest prepares sarama.CreateTopicsRequest from arguments
func (*ResourceHelper) CreateKafkaTopicRequest(
	name string,
	partitions int,
	replicationFactor int,
	replicaAssignment map[int32][]int32,
	configEntries map[string]*string,
) *sarama.CreateTopicsRequest {
	topicDetail := &sarama.TopicDetail{
		NumPartitions: int32(partitions),
		ReplicationFactor: int16(replicationFactor),
		ReplicaAssignment: replicaAssignment,
		ConfigEntries: configEntries,
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
