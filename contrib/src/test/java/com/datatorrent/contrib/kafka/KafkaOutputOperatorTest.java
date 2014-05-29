/*
 * Copyright (c) 2013 DataTorrent, Inc. ALL Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.datatorrent.contrib.kafka;
import com.datatorrent.api.ActivationListener;
import com.datatorrent.api.Context.OperatorContext;
import com.datatorrent.api.DAG;
import com.datatorrent.api.DAG.Locality;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.api.InputOperator;
import com.datatorrent.api.LocalMode;
import java.util.Properties;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import junit.framework.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 */
public class KafkaOutputOperatorTest extends KafkaOperatorTestBase
{
  private static final Logger logger = LoggerFactory.getLogger(KafkaOutputOperatorTest.class);
  private static int tupleCount = 0;
  private static final int maxTuple = 20;
  private static CountDownLatch latch;
  
   /**
   * Tuple generator for testing.
   */
  public static class StringGeneratorInputOperator implements InputOperator, ActivationListener<OperatorContext>
  {
    public final transient DefaultOutputPort<String> outputPort = new DefaultOutputPort<String>();
    private final transient ArrayBlockingQueue<String> stringBuffer = new ArrayBlockingQueue<String>(1024);
    private volatile Thread dataGeneratorThread;

    @Override
    public void beginWindow(long windowId)
    {
    }

    @Override
    public void endWindow()
    {
    }

    @Override
    public void setup(OperatorContext context)
    {
    }

    @Override
    public void teardown()
    {
    }

    @Override
    public void activate(OperatorContext ctx)
    {
      dataGeneratorThread = new Thread("String Generator")
      {
        @Override
        @SuppressWarnings("SleepWhileInLoop")
        public void run()
        {
          try {
            int i = 0;
            while (dataGeneratorThread != null && i < maxTuple) {
              stringBuffer.put("testString " + (++i));
              tupleCount++;
            }
            stringBuffer.put(KafkaOperatorTestBase.END_TUPLE);
          }
          catch (InterruptedException ie) {
          }
        }
      };
      dataGeneratorThread.start();
    }

    @Override
    public void deactivate()
    {
      dataGeneratorThread = null;
    }

    @Override
    public void emitTuples()
    {
      for (int i = stringBuffer.size(); i-- > 0;) {
        outputPort.emit(stringBuffer.poll());
      }
    }
  } // End of StringGeneratorInputOperator

  /**
   * Test AbstractKafkaOutputOperator (i.e. an output adapter for Kafka, aka producer).
   * This module sends data into an ActiveMQ message bus.
   *
   * [Generate tuple] ==> [send tuple through Kafka output adapter(i.e. producer) into Kafka message bus]
   * ==> [receive data in outside Kaka listener (i.e consumer)]
   *
   * @throws Exception
   */
  @Test
  @SuppressWarnings({"SleepWhileInLoop", "empty-statement", "rawtypes"})
  public void testKafkaOutputOperator() throws Exception
  {
    //initialize the latch to synchronize the threads
    latch = new CountDownLatch(maxTuple);
    // Setup a message listener to receive the message
    KafkaTestConsumer listener = new KafkaTestConsumer("topic1");
    listener.setLatch(latch);
    new Thread(listener).start();

    // Malhar module to send message
    // Create DAG for testing.
    LocalMode lma = LocalMode.newInstance();
    DAG dag = lma.getDAG();

    // Create ActiveMQStringSinglePortOutputOperator
    StringGeneratorInputOperator generator = dag.addOperator("TestStringGenerator", StringGeneratorInputOperator.class);
    KafkaSinglePortOutputOperator node = dag.addOperator("Kafka message producer", KafkaSinglePortOutputOperator.class);
    
    Properties props = new Properties();
    props.setProperty("serializer.class", "kafka.serializer.StringEncoder");
    props.put("metadata.broker.list", "localhost:9092");
    props.setProperty("producer.type", "async");
    props.setProperty("queue.buffering.max.ms", "200");
    props.setProperty("queue.buffering.max.messages", "10");
    props.setProperty("batch.num.messages", "5");
    
    node.setConfigProperties(props);
    // Set configuration parameters for Kafka
    node.setTopic("topic1");

    // Connect ports
    dag.addStream("Kafka message", generator.outputPort, node.inputPort).setLocality(Locality.CONTAINER_LOCAL);

    // Create local cluster
    final LocalMode.Controller lc = lma.getController();
    lc.runAsync();

    // Immediately return unless latch timeout in 5 seconds
    latch.await(15, TimeUnit.SECONDS);
    lc.shutdown();

    // Check values send vs received
    Assert.assertEquals("Number of emitted tuples", tupleCount, listener.holdingBuffer.size());
    logger.debug(String.format("Number of emitted tuples: %d", listener.holdingBuffer.size()));
    Assert.assertEquals("First tuple", "testString 1", listener.getMessage(listener.holdingBuffer.peek()));

    listener.close();
  }


}
