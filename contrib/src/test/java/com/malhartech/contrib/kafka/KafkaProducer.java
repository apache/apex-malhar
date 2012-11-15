/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package com.malhartech.contrib.kafka;

import java.util.Properties;
import kafka.javaapi.producer.Producer;
import kafka.javaapi.producer.ProducerData;
import kafka.producer.ProducerConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author Locknath Shil <locknath@malhar-inc.com>
 */
public class KafkaProducer implements Runnable
{
  private static final Logger logger = LoggerFactory.getLogger(KafkaProducer.class);
  private final kafka.javaapi.producer.Producer<Integer, String> producer;
  private final String topic;
  private int sendCount = 20;

  public int getSendCount()
  {
    return sendCount;
  }

  public void setSendCount(int sendCount)
  {
    this.sendCount = sendCount;
  }

  private ProducerConfig createProducerConfig()
  {
    Properties props = new Properties();
    props.setProperty("serializer.class", "kafka.serializer.StringEncoder");
    props.setProperty("zk.connect", "localhost:2182");
    //props.setProperty("hostname", "localhost");
    //props.setProperty("port", "2182");
    //props.setProperty("broker.list", "1:localhost:2182");
    //props.setProperty("producer.type", "async");

    return new ProducerConfig(props);
  }

  public KafkaProducer(String topic)
  {
    // Use random partitioner. Don't need the key type. Just set it to Integer.
    // The message is of type String.
    producer = new Producer<Integer, String>(createProducerConfig());
    this.topic = topic;
  }

  @Override
  public void run()
  {
    // Create dummy message
    int messageNo = 1;
    while (messageNo <= sendCount) {
      String messageStr = "Message_" + messageNo;
      producer.send(new ProducerData<Integer, String>(topic, messageStr));
      messageNo++;
      logger.debug(String.format("Producing %s", messageStr));
    }
  }

  public void close()
  {
    producer.close();
  }
} // End of KafkaProducer