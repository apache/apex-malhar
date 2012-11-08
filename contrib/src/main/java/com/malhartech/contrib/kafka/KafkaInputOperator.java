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
import com.malhartech.api.*;
import com.malhartech.api.Context.OperatorContext;
import com.malhartech.dag.SerDe;
import com.malhartech.util.CircularBuffer;
import javax.jms.JMSException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class KafkaInputOperator extends KafkaBase implements InputOperator, ActivationListener<OperatorContext>
{
    private static final Logger logger = LoggerFactory.getLogger(KafkaInputOperator.class);
  protected static final int TUPLES_BLAST_DEFAULT = 10 * 1024; // 10k
  protected static final int BUFFER_SIZE_DEFAULT = 1024 * 1024; // 1M

  // Config parameters that user can set.-
  private int tuplesBlast = TUPLES_BLAST_DEFAULT;
  private int bufferSize = BUFFER_SIZE_DEFAULT;
  protected transient CircularBuffer<Message> holdingBuffer = new CircularBuffer<Message>(bufferSize);

   public int getTuplesBlast()
  {
    return tuplesBlast;
  }

  public void setTuplesBlast(int tuplesBlast)
  {
    this.tuplesBlast = tuplesBlast;
  }

  public int getBufferSize()
  {
    return bufferSize;
  }

  public void setBufferSize(int bufferSize)
  {
    this.bufferSize = bufferSize;
  }

  /**
   * Implement abstract method of ActiveMQConsumerBase
   */
  protected final void emitMessage(Message message) throws JMSException
  {
    holdingBuffer.add(message);
  }

  /**
   * Implement Component Interface.
   *
   * @param config
   */
  @Override
  public void setup(OperatorContext context)
  {
  }

  /**
   * Implement Component Interface.
   */
  @Override
  public void teardown()
  {
  }

  /**
   * Implement Operator Interface.
   */
  @Override
  public void beginWindow(long windowId)
  {
  }

  /**
   * Implement Operator Interface.
   */
  @Override
  public void endWindow()
  {
  }

  /**
   * Implement ActivationListener Interface.
   */
  @Override
  public void activate(OperatorContext ctx)
  {
      createConsumer("mytopic");

  }

  /**
   * Implement ActivationListener Interface.
   */
  @Override
  public void deactivate()
  {
    //cleanup();
  }

  /**
   * Implement InputOperator Interface.
   */
  @Override
  public void emitTuples()
  {
    int bufferLength = holdingBuffer.size();
    for (int i = getTuplesBlast() < bufferLength ? getTuplesBlast() : bufferLength; i-- > 0;) {
      Message msg = holdingBuffer.pollUnsafe();
     // emitTuple(msg);
      //logger.debug("emitTuples() got called from {} with tuple: {}", this, msg);
    }
  }
  /*
  private ConsumerConnector consumer;
  private String topic;
  private SerDe serde;
  @OutputPortFieldAnnotation(name = "outputPort")
  final public transient DefaultOutputPort<Object> outputPort = new DefaultOutputPort<Object>(this);

  @Override
  public void setup(OperatorContext context)
  {
    Properties props = new Properties();
    String interesting[] = {
      "zk.connect",
      "zk.connectiontimeout.ms",
      "groupid",
      "topic"
    };

    throw new RuntimeException("fix the logic to populate the props in order to make it work");
//    for (String s : interesting) {
//      if (config.get(s) != null) {
//        props.put(s, config.get(s));
//      }
//    }
//
//    topic = props.containsKey("topic") ? props.getProperty("topic") : "";
//    consumer = Consumer.createJavaConsumerConnector(new ConsumerConfig(props));
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

     //get the object from message

    if (message instanceof Message) {
      ByteBuffer buffer = ((Message)message).payload();
      byte[] bytes = new byte[buffer.remaining()];
      buffer.get(bytes);

      return serde.fromByteArray(bytes);
    }

    return null;
  }

  public void emitTuples()
  {
  }

  public void beginWindow(long windowId)
  {
  }

  public void endWindow()
  {
  } */

}
