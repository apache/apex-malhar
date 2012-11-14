/*
 *  Copyright (c) 2012 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.malhartech.contrib.kafka;

import com.malhartech.api.BaseOperator;
import com.malhartech.api.DAG;
import com.malhartech.api.DefaultInputPort;
import com.malhartech.api.Operator;
import com.malhartech.stram.StramLocalCluster;
import java.io.File;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.nio.charset.CharsetDecoder;
import java.util.*;
import java.util.logging.Level;
import java.util.logging.Logger;
import junit.framework.Assert;
import kafka.api.FetchRequest;
import kafka.consumer.ConsumerConfig;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;
import kafka.javaapi.consumer.SimpleConsumer;
import kafka.javaapi.message.ByteBufferMessageSet;
import kafka.javaapi.producer.ProducerData;
import kafka.message.Message;
import kafka.message.MessageAndOffset;
import kafka.producer.ProducerConfig;
import kafka.server.KafkaConfig;
import kafka.server.KafkaServer;
import kafka.utils.Utils;
import org.apache.zookeeper.server.NIOServerCnxn;
import org.apache.zookeeper.server.ServerConfig;
import org.apache.zookeeper.server.ZooKeeperServer;
import org.apache.zookeeper.server.ZooKeeperServerMain;
import org.apache.zookeeper.server.quorum.QuorumPeerConfig;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.LoggerFactory;

public class KafkaInputOperatorTest
{
  private static final org.slf4j.Logger logger = LoggerFactory.getLogger(KafkaInputOperatorTest.class);
  public transient KafkaServer kserver;
  private static int sendCount = 20;
  private static int receiveCount = 0;
  public static HashMap<String, List<?>> collections = new HashMap<String, List<?>>();
  //public Charset charset = Charset.forName("UTF-8");
  //public CharsetDecoder decoder = charset.newDecoder();
  NIOServerCnxn.Factory standaloneServerFactory;
  private static final String zklogdir = "/tmp/zookeeper-server-data";

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
      //props.setProperty("hostname", "localhost");
      //props.setProperty("port", "2182");
      //props.setProperty("broker.list", "1:localhost:2182");
      //props.setProperty("producer.type", "async");

      props.setProperty("zk.connect", "localhost:2182");

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

    public void cleanup()
    {
      producer.close();
    }
  } // End of KafkaProducer

  public class KafkaSimpleConsumer extends Thread
  {
    // create a consumer to connect to the kafka kserver running on localhost, port 2182, socket timeout of 10 secs, socket receive buffer of ~1MB
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
      while (receiveCount < sendCount) {
        // create a fetch request for topic “topic1”, partition 0, current offset, and fetch size of 1MB
        FetchRequest fetchRequest = new FetchRequest("topic1", 0, offset, 1000000);

        // get the message set from the consumer and print them out
        ByteBufferMessageSet messages = consumer.fetch(fetchRequest);
        Iterator<MessageAndOffset> itr = messages.iterator();

        while (itr.hasNext()) {
          MessageAndOffset msg = itr.next();
          System.out.println("consumed: " + bb_to_str(msg.message().payload()).toString());

          // advance the offset after consuming each message
          offset = msg.offset();
          System.out.println(String.format("offset %d", offset));
          receiveCount++;
        }
      }
    }
  }

  public class KafkaConsumer implements Runnable // extends Thread
  {
    private final ConsumerConnector consumer;
    private final String topic;
    private boolean isAlive = true;

    public void setIsAlive(boolean isAlive)
    {
      this.isAlive = isAlive;
    }

    public KafkaConsumer(String topic)
    {
      consumer = kafka.consumer.Consumer.createJavaConsumerConnector(createConsumerConfig());
      this.topic = topic;
    }

    private ConsumerConfig createConsumerConfig()
    {
      Properties props = new Properties();
      props.setProperty("zk.connect", "localhost:2182");
      props.setProperty("groupid", "group1");
      //props.setProperty("hostname", "localhost");
      //props.setProperty("port", "2182");

      // props.put("zk.sessiontimeout.ms", "400");
      // props.put("zk.synctime.ms", "200");
      // props.put("autocommit.interval.ms", "1000");

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
      try {
        Map<String, Integer> topicCountMap = new HashMap<String, Integer>();
        topicCountMap.put(topic, new Integer(1));
        Map<String, List<KafkaStream<Message>>> consumerMap = consumer.createMessageStreams(topicCountMap);
        KafkaStream<Message> stream = consumerMap.get(topic).get(0);
        ConsumerIterator<Message> it = stream.iterator();
        System.out.println(String.format("inside consumer:run receiveCount= %d ", receiveCount));
        while (it.hasNext() & isAlive) {
          System.out.println(String.format("Consuming %s, receiveCount= %d", getMessage(it.next().message()), receiveCount));
          receiveCount++;
        }
      }
      catch (kafka.common.OffsetOutOfRangeException ex) {
        logger.warn("live issue %s", ex);
      }

      System.out.println(String.format("LSHILL Consuming is done"));
    }

    public void cleanup()
    {
      consumer.shutdown();
    }
  } // End of KafkaConsumer

  public void startZookeeper()
  {
    try {
      int clientPort = 2182;
      int numConnections = 5000;
      int tickTime = 2000;
      int maxSessionTimeout = 2000;

      File dir = new File(zklogdir);

      ZooKeeperServer zserver = new ZooKeeperServer(dir, dir, tickTime);
      standaloneServerFactory = new NIOServerCnxn.Factory(new InetSocketAddress(clientPort), numConnections);

      //zserver.setMaxSessionTimeout(maxSessionTimeout);
      standaloneServerFactory.startup(zserver); // start the zookeeper server.
    }
    catch (InterruptedException ex) {
      Logger.getLogger(KafkaInputOperatorTest.class.getName()).log(Level.SEVERE, null, ex);
    }
    catch (IOException ex) {
      Logger.getLogger(KafkaInputOperatorTest.class.getName()).log(Level.SEVERE, null, ex);
    }
  }

  public void startZookeeper2()
  {
    Properties props = new Properties();
    props.setProperty("hostname", "localhost");
    props.setProperty("clientPort", "2182");
    String dataDirectory = System.getProperty("java.io.tmpdir");
    props.setProperty("dataDir", dataDirectory);
    //  props.setProperty("brokerid", "1");
    //  props.setProperty("log.dir", "/tmp/embeddedkafka/");
    //   props.setProperty("enable.zookeeper", "true");
    //  props.setProperty("topic", "topic1");

    QuorumPeerConfig quorumConfiguration = new QuorumPeerConfig();
    try {
      quorumConfiguration.parseProperties(props);
    }
    catch (Exception e) {
      throw new RuntimeException(e);
    }

    final ZooKeeperServerMain zooKeeperServer = new ZooKeeperServerMain();
    final ServerConfig configuration = new ServerConfig();
    configuration.readFrom(quorumConfiguration);

    new Thread()
    {
      @Override
      public void run()
      {
        try {
          zooKeeperServer.runFromConfig(configuration);
          //zooKeeperServer.shutdown();
        }
        catch (IOException e) {
          logger.error("ZooKeeper Failed", e);
        }
      }
    }.start();

  }

  public void stopZookeeper()
  {
    standaloneServerFactory.shutdown();
    Utils.rm(zklogdir);
  }

  public void startKafkaServer()
  {
    Properties props = new Properties();
    //  props.setProperty("hostname", "localhost");
    //props.setProperty("port", "9092");
    props.setProperty("brokerid", "1");
    props.setProperty("log.dir", "/tmp/kafka-server-data/");
    props.setProperty("enable.zookeeper", "true");
    props.setProperty("topic", "topic1");
    props.setProperty("log.flush.interval", "10"); // Controls the number of messages accumulated in each topic (partition) before the data is flushed to disk and made available to consumers.
    //   props.setProperty("log.default.flush.scheduler.interval.ms", "100");  // optional if we have the flush.interval
    props.setProperty("zk.connect", "localhost:2182");

    kserver = new KafkaServer(new KafkaConfig(props));
    kserver.startup();
  }

  public void stopKafkaServer()
  {
    //server.getLogManager().cleanupLogs();
    // System.out.println(String.format("File %s", kserver.getLogManager().logDir().getAbsolutePath()));
    // kserver.getLogManager().logDir().deleteOnExit();
    // kserver.getLogManager().StopActor();
    kserver.CLEAN_SHUTDOWN_FILE();

    kserver.shutdown();
    kserver.awaitShutdown();
    Utils.rm(kserver.getLogManager().logDir());
  }

  @Before
  public void beforeTest()
  {
    startZookeeper();

    startKafkaServer();

  }

  @After
  public void afterTest()
  {
    collections.clear();
    stopKafkaServer();
    stopZookeeper();

  }

  @Test
  public void testKafkaProducerConsumer() throws InterruptedException
  {
    // Start producer
    KafkaProducer p = new KafkaProducer("topic1");
    p.start();
    Thread.sleep(1000);  // wait to flush message to disk and make available for consumer
    p.cleanup();

    // Start consumer
    KafkaConsumer c = new KafkaConsumer("topic1");
    //KafkaSimpleConsumer c = new KafkaSimpleConsumer();
    Thread t = new Thread(c);
    t.start();

    Thread.sleep(1000); // make sure to consume all available message

    c.setIsAlive(false);
    c.cleanup();

    // Check send vs receive message
    Assert.assertEquals("Message count: ", sendCount, receiveCount);

  }

  // ==================================
  /**
   * An example Concrete class of KafkaSinglePortInputOperator for testing.
   */
  public static class KafkaStringSinglePortInputOperator extends KafkaSinglePortInputOperator<String>
  {
    /**
     * Implement abstract method of AbstractActiveMQSinglePortInputOperator
     */
    @Override
    public String getTuple(ByteBuffer message)
    {
      String data = "";
      try {
        int old_position = message.position();
        data = decoder.decode(message).toString();
        // reset buffer's position to its original so it is not altered:
        message.position(old_position);
      }
      catch (Exception e) {
        return data;
      }
      return data;
    }
  } // End of KafkaStringSinglePortInputOperator

  /**
   * Test Operator to collect tuples from ActiveMQStringSinglePortInputOperator.
   *
   * @param <T>
   */
  public static class CollectorModule<T> extends BaseOperator
  {
    public final transient CollectorInputPort<T> inputPort = new CollectorInputPort<T>("myInput", this);
  }

  /**
   * Test Operator to collect tuples from ActiveMQMultiPortInputOperator.
   *
   * @param <T1, T2>
   */
  public static class CollectorModule2<T1, T2> extends BaseOperator
  {
    public final transient CollectorInputPort<T1> inputPort1 = new CollectorInputPort<T1>("myInput1", this);
    public final transient CollectorInputPort<T2> inputPort2 = new CollectorInputPort<T2>("myInput2", this);
  }

  public static class CollectorInputPort<T> extends DefaultInputPort<T>
  {
    ArrayList<T> list;
    final String id;

    public CollectorInputPort(String id, Operator module)
    {
      super(module);
      this.id = id;
    }

    @Override
    public void process(T tuple)
    {
      list.add(tuple);
    }

    @Override
    public void setConnected(boolean flag)
    {
      if (flag) {
        collections.put(id, list = new ArrayList<T>());
      }
    }
  }

  /**
   * Test KafkaSinglePortInputOperator (i.e. an input adapter for ActiveMQ, aka consumer).
   * This module receives data from an outside test generator through Kafka message bus and
   * feed that data into Malhar streaming platform.
   *
   * [Generate message and send that to Kafka message bus] ==>
   * [Receive that message through Kafka input adapter(i.e. consumer) and send using emitTuples() interface on output port during onMessage call]
   *
   *
   * @throws Exception
   */
  //@Test
  public void testKafkaInputOperator() throws Exception
  {
    // Start producer
    KafkaProducer p = new KafkaProducer("topic1");
    p.start();
    Thread.sleep(1000);  // wait to flush message to disk and make available for consumer


    // Create DAG for testing.
    DAG dag = new DAG();
    // Create KafkaStringSinglePortInputOperator
    KafkaStringSinglePortInputOperator node = dag.addOperator("Kafka message consumer", KafkaStringSinglePortInputOperator.class);

    // Create Test tuple collector
    CollectorModule<String> collector = dag.addOperator("TestMessageCollector", new CollectorModule<String>());

    // Connect ports
    dag.addStream("Kafka message", node.outputPort, collector.inputPort).setInline(true);

    //new Thread(node).start();

    // Create local cluster
    final StramLocalCluster lc = new StramLocalCluster(dag);
    lc.setHeartbeatMonitoringEnabled(false);

    // Run local cluster
    new Thread("LocalClusterController")
    {
      @Override
      public void run()
      {
        try {
          Thread.sleep(1000);
        }
        catch (InterruptedException ex) {
        }

        lc.shutdown();
      }
    }.start();
    lc.run();

    // Check results
    Assert.assertEquals("Collections size", 1, collections.size());
    Assert.assertEquals("Tuple count", 20, collections.get(collector.inputPort.id).size());
  }
}
