package helper

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

var helper = ResourceHelper{}

func TestCreateResourceParams(t *testing.T) {
	assert.NotNil(t, helper)
	topic, err := helper.CreateResourceParams(nil)
	assert.NotNil(t, err)
	assert.Equal(t, "", topic.Name)
	assert.Equal(t, 0, topic.Partitions)
	assert.Equal(t, 0, topic.ReplicationFactor)
}

func TestCreateKafkaTopicRequest(t *testing.T) {
	aTopic := "myTopic"
	aPartition := 1
	aReplicas := 2
	configEntries := make(map[string]*string)

	response := helper.CreateKafkaTopicRequest(aTopic, aPartition, aReplicas, configEntries)
	assert.NotEmpty(t, response)
	assert.NotNil(t, response)

	assert.NotNil(t, response.TopicDetails[aTopic])
	assert.Equal(t, aPartition, int(response.TopicDetails[aTopic].NumPartitions))
	assert.Equal(t, aReplicas, int(response.TopicDetails[aTopic].ReplicationFactor))
}

func TestDeleteKafkaTopicRequest(t *testing.T) {
	topic := "myTopic"

	response := helper.DeleteKafkaTopicRequest(topic)
	assert.NotEmpty(t, response)
	assert.NotNil(t, response)
	assert.NotNil(t, response.Topics)
	assert.Equal(t, topic, response.Topics[0])
}

func TestCreateKafkaPartitionRequest(t *testing.T) {
	topic := "mytopic"
	partition := int32(3)

	res := helper.CreateKafkaPartitionRequest(topic, partition)
	assert.NotEmpty(t, res)
	assert.NotNil(t, res)
	assert.Equal(t, res.TopicPartitions[topic].Count, partition)
}

func TestGetKafkaMetadataRequest(t *testing.T) {
	topic := "mytopic"
	res := helper.GetKafkaMetadataRequest(topic)
	assert.NotEmpty(t, res)
	assert.NotNil(t, res)
	assert.Equal(t, res.Topics[0], topic)
}
