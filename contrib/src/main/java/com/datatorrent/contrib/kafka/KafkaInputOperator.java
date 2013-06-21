/*
 *  Copyright (c) 2012 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.datatorrent.contrib.kafka;

import com.datatorrent.api.annotation.ShipContainingJars;
import com.datatorrent.api.ActivationListener;
import com.datatorrent.api.Context.OperatorContext;
import com.datatorrent.api.InputOperator;
import java.security.InvalidParameterException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
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

/**
 * Kafka input adapter operator, which consume data from Kafka message bus.<p><br>
 *
 * <br>
 * Ports:<br>
 * <b>Input</b>: No input port<br>
 * <b>Output</b>: Can have any number of output ports<br>
 * <br>
 * Properties:<br>
 * <b>tuplesBlast</b>: Number of tuples emitted in each burst<br>
 * <b>bufferSize</b>: Size of holding buffer<br>
 * <br>
 * Compile time checks:<br>
 * Class derived from this has to implement the abstract method emitTuple() <br>
 * <br>
 * Run time checks:<br>
 * None<br>
 * <br>
 * Benchmarks:<br>
 * TBD<br>
 * <br>
 * @author Locknath Shil <locknath@malhar-inc.com>
 *
 */
@ShipContainingJars(classes={kafka.javaapi.consumer.SimpleConsumer.class, /*org.I0Itec.zkclient.ZkClient.class,*/ scala.ScalaObject.class})
public abstract class KafkaInputOperator implements InputOperator, ActivationListener<OperatorContext>
{
  @SuppressWarnings("unused")
  private static final Logger logger = LoggerFactory.getLogger(KafkaInputOperator.class);
  protected static final int TUPLES_BLAST_DEFAULT = 10 * 1024; // 10k
  protected static final int BUFFER_SIZE_DEFAULT = 1024 * 1024; // 1M
  // Config parameters that user can set.
  private int tuplesBlast = TUPLES_BLAST_DEFAULT;
  private int bufferSize = BUFFER_SIZE_DEFAULT;
  protected transient ArrayBlockingQueue<Message> holdingBuffer;
  private transient ConsumerConnector standardConsumer;
  private transient SimpleConsumer simpleConsumer;
  private transient Thread consumerThread;
  private boolean isAlive = true;
  private transient KafkaConsumer consumer;
  @NotNull
  private String consumerType = "standard"; // can be standard, simple, console
  @NotNull
  private String topic = "topic1";
  private int numStream = 1;

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

 // @Pattern(regexp = "standard|simple|console", message = "Consumer type has to be standard, or simple, or console")
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

  public int getNumStream()
  {
    return numStream;
  }

  public void setNumStream(int numStream)
  {
    this.numStream = numStream;
  }

  /**
   * Implement Component Interface.
   *
   * @param context
   */
  @Override
  public void setup(OperatorContext context)
  {
    holdingBuffer = new ArrayBlockingQueue<Message>(bufferSize);
  }

  /**
   * Implement Component Interface.
   */
  @Override
  public void teardown()
  {
    holdingBuffer.clear();
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
    else if ("console".equals(consumerType)) {
      consumer = null;
      throw new InvalidParameterException("console type is not supported yet.");
    }
    else {
      consumer = null;
      throw new InvalidParameterException("Consumer type should be standard, or simple, or console");
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
      emitTuple(holdingBuffer.poll());
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
    @Override
    public void create()
    {
      standardConsumer = kafka.consumer.Consumer.createJavaConsumerConnector(createKafkaConsumerConfig());
    }

    @Override
    public void start()
    {
      Map<String, Integer> topicCountMap = new HashMap<String, Integer>();
      topicCountMap.put(topic, new Integer(numStream)); // take care int, how to handle multiple topics
      Map<String, List<KafkaStream<Message>>> consumerMap = standardConsumer.createMessageStreams(topicCountMap); // there is another api createMessageStreamsByFilter
      for (int i = 0; i < numStream; ++i) {
        KafkaStream<Message> stream = consumerMap.get(topic).get(i);
        ConsumerIterator<Message> itr = stream.iterator();
        while (itr.hasNext() && isAlive) {
          holdingBuffer.add(itr.next().message());
        }
      }
    }

    @Override
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
