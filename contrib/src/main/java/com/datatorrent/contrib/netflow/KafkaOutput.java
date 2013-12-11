package com.datatorrent.contrib.netflow;

import java.util.Properties;

import javax.validation.constraints.NotNull;

import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;

public class KafkaOutput<V>
{

  private transient kafka.javaapi.producer.Producer<String, V> producer; // K is key partitioner, V is value type
  @NotNull
  private String topic = "test";
  
  Properties configProperties;

  public Producer<String, V> getProducer()
  {
    return producer;
  }

  public String getTopic()
  {
    return topic;
  }

  public void setTopic(String topic)
  {
    this.topic = topic;
  }

  public KafkaOutput(Properties configProperties)
  {
    this.configProperties = configProperties;
    producer = new Producer<String, V>(new ProducerConfig(configProperties));
  }

  public Properties getConfigProperties()
  {
    return configProperties;
  }

  public void setConfigProperties(Properties configProperties)
  {
    this.configProperties = configProperties;
  }
  public void close(){
    producer.close();
  }

  public void process(V tuple)
  {
    producer.send(new KeyedMessage<String, V>(topic, tuple));
  }

  
}
