/*
 *  Copyright (c) 2012 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.malhartech.contrib.kafka;

import com.malhartech.api.ActivationListener;
import com.malhartech.api.Context.OperatorContext;
import com.malhartech.api.InputOperator;
import com.malhartech.util.CircularBuffer;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import javax.validation.constraints.NotNull;
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

public abstract class KafkaInputOperator implements InputOperator, ActivationListener<OperatorContext>
{
  private static final Logger logger = LoggerFactory.getLogger(KafkaInputOperator.class);
  protected static final int TUPLES_BLAST_DEFAULT = 10 * 1024; // 10k
  protected static final int BUFFER_SIZE_DEFAULT = 1024 * 1024; // 1M
  // Config parameters that user can set.
  private int tuplesBlast = TUPLES_BLAST_DEFAULT;
  private int bufferSize = BUFFER_SIZE_DEFAULT;
  protected transient CircularBuffer<Message> holdingBuffer = new CircularBuffer<Message>(bufferSize);
  private int receiveCount = 0;
  private transient ConsumerConnector standardConsumer;
  private transient SimpleConsumer simpleConsumer;
  private transient Thread consumerThread;
  private boolean isAlive = true;
  private transient KafkaConsumer consumer;
  @NotNull
  String consumerType = "standard"; // can be standard, simple, console
  @NotNull
  private String topic = "topic1";

  /**
   * Any concrete class derived from KafkaInputOperator has to implement this method
   * so that it knows what type of message it is going to send to Malhar in which output port.
   *
   * @param message
   */
  protected abstract void emitTuple(Message message);

  public abstract ConsumerConfig createKafkaConsumerConfig();

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

  public String getConsumerType()
  {
    return consumerType;
  }

  public void setConsumerType(String consumerType)
  {
    this.consumerType = consumerType;
  }

  public String getTopic()
  {
    return topic;
  }

  public void setTopic(String topic)
  {
    this.topic = topic;
  }

  public boolean isIsAlive()
  {
    return isAlive;
  }

  public void setIsAlive(boolean isAlive)
  {
    this.isAlive = isAlive;
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
    if ("standard".equals(consumerType)) {
      consumer = new StandardKafkaConsumer();
    }
    else if ("simple".equals(consumerType)) {
      consumer = new SimpleKafkaConsumer();
    }
    else {
      consumer = null;
      //throw new illegalArgument
    }

    consumer.create();
    consumerThread = new Thread("KafkaConsumerThread")
    {
      @Override
      public void run()
      {
        consumer.start();
      }
    };

    consumerThread.start();
  }

  /**
   * Implement ActivationListener Interface.
   */
  @Override
  public void deactivate()
  {
    consumer.stop();
  }

  /**
   * Implement InputOperator Interface.
   */
  @Override
  public void emitTuples()
  {
    int bufferLength = holdingBuffer.size();
    for (int i = tuplesBlast < bufferLength ? tuplesBlast : bufferLength; i-- > 0;) {
      emitTuple(holdingBuffer.pollUnsafe());
    }
  }

  public abstract class KafkaConsumer
  {
    public abstract void create();

    public abstract void start();

    public abstract void stop();
  }

  public class StandardKafkaConsumer extends KafkaConsumer
  {
    public void create()
    {
      standardConsumer = kafka.consumer.Consumer.createJavaConsumerConnector(createKafkaConsumerConfig());
    }

    public void start()
    {
      Map<String, Integer> topicCountMap = new HashMap<String, Integer>();
      topicCountMap.put(topic, new Integer(1)); // take care int, how to handle multiple topics
      Map<String, List<KafkaStream<Message>>> consumerMap = standardConsumer.createMessageStreams(topicCountMap); // there is another api createMessageStreamsByFilter
      KafkaStream<Message> stream = consumerMap.get(topic).get(0);
      ConsumerIterator<Message> itr = stream.iterator();
      while (itr.hasNext() && isAlive) {
        holdingBuffer.add(itr.next().message());
      }
    }

    public void stop()
    {
      isAlive = false;
      standardConsumer.shutdown();
    }
  } // End of StandardKafkaConsumer

  public class SimpleKafkaConsumer extends KafkaConsumer
  {
    @Override
    public void create()
    {
      simpleConsumer = new SimpleConsumer("localhost", 2182, 10000, 1024000);
    }

    @Override
    public void start()
    {
      long offset = 0;
      while (isAlive) {
        // create a fetch request for topic, partition 0, current offset, and fetch size of 1MB
        FetchRequest fetchRequest = new FetchRequest(topic, 0, offset, 1000000);

        // get the message set from the broker
        ByteBufferMessageSet messages = simpleConsumer.fetch(fetchRequest);
        Iterator<MessageAndOffset> itr = messages.iterator();

        while (itr.hasNext() && isAlive) {
          MessageAndOffset msg = itr.next();
          holdingBuffer.add(msg.message());
          // advance the offset after consuming each message
          offset = msg.offset();
          receiveCount++;
        }
      }
    }

    @Override
    public void stop()
    {
      isAlive = false;
      simpleConsumer.close();
    }
  }  // End of SimpleKafkaConsumer
}
