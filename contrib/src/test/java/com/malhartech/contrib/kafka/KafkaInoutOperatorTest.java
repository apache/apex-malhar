/*
 *  Copyright (c) 2012 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.malhartech.contrib.kafka;

import java.io.File;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.nio.charset.CharsetDecoder;
import java.util.*;
import kafka.consumer.ConsumerConfig;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;
import kafka.javaapi.producer.ProducerData;
import kafka.producer.ProducerConfig;
import kafka.server.KafkaConfig;
import kafka.server.KafkaServer;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import kafka.api.FetchRequest;
import kafka.javaapi.consumer.SimpleConsumer;
import kafka.javaapi.message.ByteBufferMessageSet;
import kafka.message.Message;
import kafka.message.MessageAndOffset;
import kafka.utils.Utils;

public class KafkaInoutOperatorTest
{
  public KafkaServer server;

  /**
   *
   */
  public class KafkaProducer extends Thread
  {
    private final kafka.javaapi.producer.Producer<Integer, String> producer;
    private final String topic;
    private final Properties props = new Properties();

    public KafkaProducer(String topic)
    {
      // SyncProducer by default
      props.setProperty("serializer.class", "kafka.serializer.StringEncoder");
      props.setProperty("hostname", "localhost");
      props.setProperty("port", "2182");
      props.setProperty("broker.list", "1:localhost:2182");

      //props.setProperty("zk.connect", "localhost:2181");
      // Use random partitioner. Don't need the key type. Just set it to Integer.
      // The message is of type String.
      producer = new kafka.javaapi.producer.Producer<Integer, String>(new ProducerConfig(props));
      this.topic = topic;
    }

    @Override
    public void run()
    {
      int messageNo = 1;
      int maxMessage = 20;
      while (messageNo <= maxMessage) {
        String messageStr = "Message_" + messageNo;
        producer.send(new ProducerData<Integer, String>(topic, messageStr));
        messageNo++;
        System.out.println(String.format("Producing %s", messageStr));
      }
    }
  } // End of KafkaProducer

  public class KafkaSimpleConsumer extends Thread
  {
    // create a consumer to connect to the kafka server running on localhost, port 2182, socket timeout of 10 secs, socket receive buffer of ~1MB
    SimpleConsumer consumer = new SimpleConsumer("localhost", 2182, 10000, 1024000);
    public Charset charset = Charset.forName("UTF-8");
    public CharsetDecoder decoder = charset.newDecoder();

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

    @Override
    public void run()
    {
      long offset = 0;
      while (true) {
        // create a fetch request for topic “topic1”, partition 0, current offset, and fetch size of 1MB
        FetchRequest fetchRequest = new FetchRequest("topic1", 0, offset, 1000000);

        // get the message set from the consumer and print them out
        ByteBufferMessageSet messages = consumer.fetch(fetchRequest);
        Iterator<MessageAndOffset> itr = messages.iterator();

        while (itr.hasNext()) {
          MessageAndOffset msg = itr.next();
          System.out.println("consumed: " + bb_to_str(msg.message().payload()).toString());
          // System.out.println("consumed: " + Utils.toString(msg.message.payload(), "UTF-8"));
          // advance the offset after consuming each message
          offset = msg.offset();
          System.out.println(String.format("offset %d", offset));
        }
      }
    }
  }

  public class KafkaConsumer extends Thread
  {
    private final ConsumerConnector consumer;
    private final String topic;

    public KafkaConsumer(String topic)
    {
      consumer = kafka.consumer.Consumer.createJavaConsumerConnector(createConsumerConfig());
      this.topic = topic;
    }

    private ConsumerConfig createConsumerConfig()
    {
      Properties props = new Properties();
      //props.setProperty("zk.connect", "localhost:2181");
      props.setProperty("groupid", "group1");
      props.setProperty("hostname", "localhost");
      props.setProperty("port", "2182");

      //props.put("zk.sessiontimeout.ms", "400");
      // props.put("zk.synctime.ms", "200");
      //props.put("autocommit.interval.ms", "1000");

      return new ConsumerConfig(props);

    }

    public String getMessage(Message message)
    {
      ByteBuffer buffer = message.payload();
      byte[] bytes = new byte[buffer.remaining()];
      buffer.get(bytes);
      return new String(bytes);
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
        System.out.println(String.format("Consuming %s", getMessage(it.next().message())));
      }
    }
  } // End of KafkaConsumer

  @Before
  public void beforeTest()
  {
    Properties props = new Properties();
    props.setProperty("hostname", "localhost");
    props.setProperty("port", "2182");
    props.setProperty("brokerid", "1");
    props.setProperty("log.dir", "/tmp/embeddedkafka/");
    props.setProperty("enable.zookeeper", "false");
    props.setProperty("topic", "topic1");
    //props.setProperty("zk.connect", "localhost:2181");

    server = new KafkaServer(new KafkaConfig(props));
    server.startup();

  }

  @After
  public void afterTest()
  {
    //server.getLogManager().cleanupLogs();
   // System.out.println(String.format("File %s", server.getLogManager().logDir().getAbsolutePath()));
   // server.getLogManager().logDir().deleteOnExit();
   // server.getLogManager().StopActor();
    //server.CLEAN_SHUTDOWN_FILE();

    server.shutdown();
    //server.awaitShutdown();
    Utils.rm(server.getLogManager().logDir());
  }

  @Test
  public void testKafkaProducer() throws InterruptedException
  {
    KafkaProducer p = new KafkaProducer("topic1");
    p.start();
    Thread.sleep(1000);
    // KafkaConsumer c = new KafkaConsumer("topic1");
    KafkaSimpleConsumer c = new KafkaSimpleConsumer();
    c.start();
    System.out.println();
    Thread.sleep(1000);
  }
}
