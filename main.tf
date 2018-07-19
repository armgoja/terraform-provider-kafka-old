resource "kafka_topic" "my-topic" {
  name = "my-topic43"
  partitions = 1
  replication_factor = 1
}
