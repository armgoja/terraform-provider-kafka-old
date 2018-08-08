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
		"cleanup_policy": &schema.Schema{
			Type:        schema.TypeString,
			Description: "This string designates the retention policy to use on old log segments.",
			Optional:    true,
			Default:     "delete",
		},
		"compression_type": &schema.Schema{
			Type:        schema.TypeString,
			Description: "Specify the final compression type for a given topic.",
			Optional:    true,
			Default:     "producer",
		},
		"max_message_bytes": &schema.Schema{
			Type:        schema.TypeInt,
			Description: "The largest record batch size allowed by Kafka.",
			Optional:    true,
			Default:     1000012,
		},
		"message_timestamp_type": &schema.Schema{
			Type:        schema.TypeString,
			Description: "Define whether the timestamp in the message is message create time or log append time.",
			Optional:    true,
			Default:     "CreateTime",
		},
		"message_timestamp_difference_max_ms": &schema.Schema{
			Type:        schema.TypeInt,
			Description: "The maximum difference allowed between the timestamp when a broker receives a message and the timestamp specified in the message.",
			Optional:    true,
			Default:     9223372036854775807,
		},
		"segment_bytes": &schema.Schema{
			Type:        schema.TypeInt,
			Description: "This configuration controls the segment file size for the log.",
			Optional:    true,
			Default:     1073741824,
		},
		"segment_index_bytes": &schema.Schema{
			Type:        schema.TypeInt,
			Description: "This configuration controls the size of the index that maps offsets to file positions.",
			Optional:    true,
			Default:     10485760,
		},
		"segment_jitter_ms": &schema.Schema{
			Type:        schema.TypeInt,
			Description: "The maximum random jitter subtracted from the scheduled segment roll time to avoid thundering herds of segment rolling.",
			Optional:    true,
			Default:     0,
		},
		"segment_ms": &schema.Schema{
			Type:        schema.TypeInt,
			Description: "This configuration controls the period of time after which Kafka will force the log to roll even if the segment file isn't full to ensure that retention can delete or compact old data.",
			Optional:    true,
			Default:     604800000,
		},
		"index_interval_bytes": &schema.Schema{
			Type:        schema.TypeInt,
			Description: "This setting controls how frequently Kafka adds an index entry to it's offset index.",
			Optional:    true,
			Default:     4096,
		},
		"retention_bytes": &schema.Schema{
			Type:        schema.TypeInt,
			Description: `This configuration controls the maximum size a partition (which consists of log segments) can grow to before we will discard old log segments to free up space if we are using the "delete" retention policy.`,
			Optional:    true,
			Default:     -1,
		},
		"retention_ms": &schema.Schema{
			Type:        schema.TypeInt,
			Description: `This configuration controls the maximum time we will retain a log before we will discard old log segments to free up space if we are using the "delete" retention policy.`,
			Optional:    true,
			Default:     604800000,
		},
		"delete_retention_ms": &schema.Schema{
			Type:        schema.TypeInt,
			Description: "The amount of time to retain delete tombstone markers for log compacted topics.",
			Optional:    true,
			Default:     86400000,
		},
		"file_delete_delay_ms": &schema.Schema{
			Type:        schema.TypeInt,
			Description: "The time to wait before deleting a file from the filesystem",
			Optional:    true,
			Default:     60000,
		},
		"flush_messages": &schema.Schema{
			Type:        schema.TypeInt,
			Description: "This setting allows specifying an interval at which we will force an fsync of data written to the log.",
			Optional:    true,
			Default:     9223372036854775807,
		},
		"flush_ms": &schema.Schema{
			Type:        schema.TypeInt,
			Description: "This setting allows specifying a time interval at which we will force an fsync of data written to the log.",
			Optional:    true,
			Default:     9223372036854775807,
		},
		"min_cleanable_dirty_ratio": &schema.Schema{
			Type:        schema.TypeFloat,
			Description: `This setting controls how frequently Kafka adds an index entry to it's offset index.`,
			Optional:    true,
			Default:     0.5,
		},
		"min_compaction_lag_ms": &schema.Schema{
			Type:        schema.TypeInt,
			Description: `The minimum time a message will remain uncompacted in the log.`,
			Optional:    true,
			Default:     0,
		},
		"min_insync_replicas": &schema.Schema{
			Type:        schema.TypeInt,
			Description: `When a producer sets acks to "all" (or "-1"), this configuration specifies the minimum number of replicas that must acknowledge a write for the write to be considered successful.`,
			Optional:    true,
			Default:     1,
		},
		"message_format_version": &schema.Schema{
			Type:        schema.TypeString,
			Description: "Specify the message format version the broker will use to append messages to the logs.",
			Optional:    true,
			Default:     "2.0-IV1",
		},
		"follower_replication_throttled_replicas": &schema.Schema{
			Type:        schema.TypeString,
			Description: "A list of replicas for which log replication should be throttled on the follower side.",
			Optional:    true,
			Default:     "",
		},
		"leader_replication_throttled_replicas": &schema.Schema{
			Type:        schema.TypeString,
			Description: "A list of replicas for which log replication should be throttled on the leader side.",
			Optional:    true,
			Default:     "",
		},
		"preallocate": &schema.Schema{
			Type:        schema.TypeBool,
			Description: "True if we should preallocate the file on disk when creating a new log segment.",
			Optional:    true,
			Default:     false,
		},
		"unclean_leader_election_enable": &schema.Schema{
			Type:        schema.TypeBool,
			Description: "Indicates whether to enable replicas not in the ISR set to be elected as leader as a last resort, even though doing so may result in data loss.",
			Optional:    true,
			Default:     false,
		},
		"message_downconversion_enable": &schema.Schema{
			Type:        schema.TypeBool,
			Description: "This configuration controls whether down-conversion of message formats is enabled to satisfy consume requests.",
			Optional:    true,
			Default:     true,
		},
	}
}

func resourceKafkaTopicCreate(d *schema.ResourceData, m interface{}) error {
	broker := m.(*sarama.Broker)
	defer broker.Close()

	// Get basic topic properties from input
	name, partition, replicationFactor, configEntries, err := r.CreateResourceParams(d)

	// TODO: define replica assignment on input schema
    var replicaAssignment map[int32][]int32

	// Prepare CreateTopicRequest
	topicRequest := r.CreateKafkaTopicRequest(
		name,
		partition,
		replicationFactor,
		replicaAssignment,
		configEntries,
	)

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
	broker := m.(*sarama.Broker)
	defer broker.Close()

	topic := d.Get("name").(string)

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
	broker := m.(*sarama.Broker)
	defer broker.Close()

	topic := d.Get("name").(string)

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
	return nil
}

func resourceKafkaTopicDelete(d *schema.ResourceData, m interface{}) error {
	broker := m.(*sarama.Broker)
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
