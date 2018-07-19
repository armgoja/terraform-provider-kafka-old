resource "kafka_topic" "my-topic" {
  name = "my-topic1"
  partitions = 1
  replication_factor = 1
}
