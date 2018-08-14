provider "kafka" {
  broker_list = "localhost:29092"
}

variable "topic_configs_default" {
  type = "map"

  default = {
    "cleanup.policy"                          = "delete"
    "compression.type"                        = "producer"
    "delete.retention.ms"                     = "86400000"
    "file.delete.delay.ms"                    = "60000"
    "flush.messages"                          = "9223372036854775807"
    "flush.ms"                                = "9223372036854775807"
    "follower.replication.throttled.replicas" = ""
    "index.interval.bytes"                    = "4096"
    "leader.replication.throttled.replicas"   = ""
    "max.message.bytes"                       = "1000012"
    "message.downconversion.enable"           = "true"
    "message.format.version"                  = "2.0-IV1"
    "message.timestamp.difference.max.ms"     = "9223372036854775807"
    "message.timestamp.type"                  = "CreateTime"
    "min.cleanable.dirty.ratio"               = "0.5"
    "min.insync.replicas"                     = "1"
    "preallocate"                             = "false"
    "retention.bytes"                         = "-1"
    "retention.ms"                            = "604800000"
    "segment.bytes"                           = "1073741824"
    "segment.index.bytes"                     = "10485760"
    "segment.jitter.ms"                       = "0"
    "segment.ms"                              = "604800000"
    "unclean.leader.election.enable"          = "false"
  }
}

resource "kafka_topic" "test-3" {
  # (resource arguments)
  config_entries = "${merge(var.topic_configs_default, map(
    "cleanup.policy", "compact",
    "compression.type", "gzip"
  ))}"

  name               = "test-3"
  partitions         = 1
  replication_factor = 1
}

resource "kafka_topic" "test-2" {
  name               = "test-2"
  partitions         = 10
  replication_factor = 1

  config_entries = "${var.topic_configs_default}"
}
