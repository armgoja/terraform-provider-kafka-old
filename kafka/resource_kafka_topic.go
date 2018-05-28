package kafka

import (
  "github.com/hashicorp/terraform/helper/schema"
)

func resourceKafkaTopic() *schema.Resource {
  return &schema.Resource {
    Create: resourceKafkaTopicCreate,
    Read:   resourceKafkaTopicRead,
    Update: resourceKafkaTopicUpdate,
    Delete: resourceKafkaTopicDelete,

    Schema: map[string]*schema.Schema{
      "name": &schema.Schema{
        Type:     schema.TypeString,
        Required: true,
      },
      "partitions": &schema.Schema{
        Type:     schema.TypeInt,
        Required: true,
      },
    },
  }
}

func resourceKafkaTopicCreate(d *schema.ResourceData, m interface{}) error {
  name := d.Get("name").(string)
  d.SetId(name)
  return nil
}

func resourceKafkaTopicRead(d *schema.ResourceData, m interface{}) error {
  return nil
}

func resourceKafkaTopicUpdate(d *schema.ResourceData, m interface{}) error {
  return nil
}

func resourceKafkaTopicDelete(d *schema.ResourceData, m interface{}) error {
  return nil
}
