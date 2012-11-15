/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package com.malhartech.contrib.kafka;

import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.nio.charset.CharsetDecoder;
import java.util.*;
import kafka.api.FetchRequest;
import kafka.consumer.ConsumerConfig;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;
import kafka.javaapi.consumer.SimpleConsumer;
import kafka.javaapi.message.ByteBufferMessageSet;
import kafka.message.Message;
import kafka.message.MessageAndOffset;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author Locknath Shil <locknath@malhar-inc.com>
 */
public abstract class KafkaBase
{
  private static final Logger logger = LoggerFactory.getLogger(KafkaBase.class);
  private transient SimpleConsumer simpleConsumer;
  private transient ConsumerConnector consumer;
  private int sendCount = 20;
  private int receiveCount = 0;
  public transient Charset charset = Charset.forName("UTF-8");
  public transient CharsetDecoder decoder = charset.newDecoder();
  private Thread simpleConsumerThread;
  private Thread consumerThread;
  private boolean isAlive = true;
  private String zkConnect = "127.0.0.1:2182";
  private String groupId = "group1";
  private String topic = "topic1";
  private String kafkaServerURL = "localhost";
  private int kafkaServerPort = 9092;
  private int kafkaProducerBufferSize = 64 * 1024;
  private int connectionTimeOut = 100000;
  private int reconnectInterval = 10000;
  private String topic2 = "topic2";
  private String topic3 = "topic3";

  public abstract void emitMessage(Message message);

  public boolean isIsAlive()
  {
    return isAlive;
  }

  public void setIsAlive(boolean isAlive)
  {
    this.isAlive = isAlive;
  }

  public void createSimpleConsumer()
  {
    simpleConsumer = new SimpleConsumer("localhost", 2182, 10000, 1024000);
  }

  public String bb_to_str(ByteBuffer buffer)
  {
    String data = "";
    try {
      int old_position = buffer.position();
      data = decoder.decode(buffer).toString();
      // reset buffer's position to its original so it is not altered:
      buffer.position(old_position);
    }
    catch (Exception e) {
      return data;
    }
    return data;
  }

  public void simpleConsumerOnMessage()
  {
    simpleConsumerThread = new Thread("SimpleConsumerThread")
    {
      @Override
      public void run()
      {
        long offset = 0;
        boolean isAlive = true;
        //while (receiveCount < sendCount) {
        while (isAlive) {
          // create a fetch request for topic “topic1”, partition 0, current offset, and fetch size of 1MB
          FetchRequest fetchRequest = new FetchRequest("topic1", 0, offset, 1000000);

          // get the message set from the consumer and print them out
          ByteBufferMessageSet messages = simpleConsumer.fetch(fetchRequest);
          Iterator<MessageAndOffset> itr = messages.iterator();

          while (itr.hasNext()) {
            MessageAndOffset msg = itr.next();
            emitMessage(msg.message());
            System.out.println("consumed: " + bb_to_str(msg.message().payload()).toString());

            // advance the offset after consuming each message
            offset = msg.offset();
            //System.out.println(String.format("offset %d", offset));
            receiveCount++;
          }
          if (Thread.interrupted()) {
            isAlive = false;
          }
        }
      }
    };
    simpleConsumerThread.start();

  }

  public void createConsumer(String topic)
  {
    Properties props = new Properties();
    props.put("zk.connect", zkConnect);
    props.put("groupid", groupId);
    //props.put("zk.sessiontimeout.ms", "400");
    // props.put("zk.synctime.ms", "200");
    //  props.put("autocommit.interval.ms", "1000");
    consumer = kafka.consumer.Consumer.createJavaConsumerConnector(new ConsumerConfig(props));
    this.topic = topic;
  }

  public String getMessage(Message message)
  {
    ByteBuffer buffer = message.payload();
    byte[] bytes = new byte[buffer.remaining()];
    buffer.get(bytes);
    return new String(bytes);
  }

  public void onMessage()
  {
    consumerThread = new Thread("KafkaConsumerThread")
    {
      @Override
      public void run()
      {
        Map<String, Integer> topicCountMap = new HashMap<String, Integer>();
        topicCountMap.put(topic, new Integer(1));
        Map<String, List<KafkaStream<Message>>> consumerMap = consumer.createMessageStreams(topicCountMap);
        KafkaStream<Message> stream = consumerMap.get(topic).get(0);
        ConsumerIterator<Message> itr = stream.iterator();
        while (itr.hasNext() && isAlive) {
          Message msg = itr.next().message();
          emitMessage(msg);
          logger.debug("Consuming {}", getMessage(msg));
        }
      }
    };

    consumerThread.start();
  }

  public void cleanup()
  {
    // simpleConsumerThread.interrupt();
    //consumerThread.interrupt();
    isAlive = false;
    consumer.shutdown();
    //simpleConsumer.close();
  }
}
