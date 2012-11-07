/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package com.malhartech.contrib.kafka;

import java.util.Properties;
import kafka.consumer.ConsumerConfig;
import kafka.javaapi.consumer.ConsumerConnector;

/**
 *
 * @author Locknath Shil <locknath@malhar-inc.com>
 */
public class KafkaBase
{
  private String zkConnect = "127.0.0.1:2181";
  private String groupId = "group1";
  private String topic = "topic1";
  private String kafkaServerURL = "localhost";
  private int kafkaServerPort = 9092;
  private int kafkaProducerBufferSize = 64 * 1024;
  private int connectionTimeOut = 100000;
  private int reconnectInterval = 10000;
  private String topic2 = "topic2";
  private String topic3 = "topic3";

  public void createConsumer(String topic)
  {
    Properties props = new Properties();
    props.put("zk.connect", zkConnect);
    props.put("groupid", groupId);
    props.put("zk.sessiontimeout.ms", "400");
    props.put("zk.synctime.ms", "200");
    props.put("autocommit.interval.ms", "1000");
    ConsumerConnector    consumer = kafka.consumer.Consumer.createJavaConsumerConnector(new ConsumerConfig(props));
    this.topic = topic;
  }

  
}
