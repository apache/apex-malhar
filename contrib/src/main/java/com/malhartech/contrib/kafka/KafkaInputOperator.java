/*
 *  Copyright (c) 2012 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.malhartech.contrib.kafka;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import kafka.consumer.Consumer;
import kafka.consumer.ConsumerConfig;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;
import kafka.message.Message;

import com.malhartech.annotation.OutputPortFieldAnnotation;
import com.malhartech.api.BaseOperator;
import com.malhartech.api.DefaultOutputPort;
import com.malhartech.api.OperatorConfiguration;
import com.malhartech.api.SyncInputOperator;
import com.malhartech.dag.SerDe;

public class KafkaInputOperator extends BaseOperator implements SyncInputOperator, Runnable
{
  private ConsumerConnector consumer;
  private String topic;
  private SerDe serde;

  @OutputPortFieldAnnotation(name="outputPort")
  final public transient DefaultOutputPort<Object> outputPort = new DefaultOutputPort<Object>(this);

  @Override
  public Runnable getDataPoller() {
    return this;
  }

  @Override
  public void setup(OperatorConfiguration config)
  {
    Properties props = new Properties();
    String interesting[] = {
      "zk.connect",
      "zk.connectiontimeout.ms",
      "groupid",
      "topic"
    };

    for (String s : interesting) {
      if (config.get(s) != null) {
        props.put(s, config.get(s));
      }
    }

    topic = props.containsKey("topic") ? props.getProperty("topic") : "";
    consumer = Consumer.createJavaConsumerConnector(new ConsumerConfig(props));
  }

  @Override
  public void run()
  {
    Map<String, Integer> topicCountMap = new HashMap<String, Integer>();
    topicCountMap.put(topic, new Integer(1));
    Map<String, List<KafkaStream<Message>>> consumerMap = consumer.createMessageStreams(topicCountMap);
    KafkaStream<Message> stream = consumerMap.get(topic).get(0);
    ConsumerIterator<Message> it = stream.iterator();
    while (it.hasNext()) {
      outputPort.emit(getObject(it.next().message()));
    }
  }

  @Override
  public void teardown()
  {
    consumer.shutdown();
    consumer = null;
    topic = null;
    new Thread(this).start();
  }

  public Object getObject(Object message)
  {
    /*
     * get the object from message
     */
    if (message instanceof Message) {
      ByteBuffer buffer = ((Message) message).payload();
      byte[] bytes = new byte[buffer.remaining()];
      buffer.get(bytes);

      return serde.fromByteArray(bytes);
    }

    return null;
  }

}
