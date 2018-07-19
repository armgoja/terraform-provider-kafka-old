package kafka

import (
	"testing"

	"github.com/stretchr/testify/assert"

	sch "github.com/hashicorp/terraform/helper/schema"
)

func TestKafkaSchema(t *testing.T) {
	schema := kafkaSchema()
	assert.NotNil(t, schema)
	assert.NotEmpty(t, schema)
	for key, val := range schema {
		if key == "name" {
			assert.Equal(t, val.Required, true)
			assert.Equal(t, val.Type, sch.TypeString)
			assert.Empty(t, val.Removed)
		}

		if key == "partitions" {
			assert.Equal(t, val.Required, true)
			assert.Empty(t, val.Removed)
			assert.Equal(t, val.Type, sch.TypeInt)
		}
	}
}

func TestCreateTopicConfig(t *testing.T) {

	name := "my-topic"
	partition := 2
	replicationFactor := 1
	topicConfig := createTopicConfig(name, partition, replicationFactor)

	assert.Equal(t, name, topicConfig.Topic)
	assert.Equal(t, partition, topicConfig.NumPartitions)
	assert.Equal(t, replicationFactor, topicConfig.ReplicationFactor)
}
