resource "kafka_topic" "my-topic" {
  name = "my-topic"
  partitions = 1
  replication_factor = 2
}
