/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.apex.malhar.contrib.kafka;

import java.util.Properties;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datatorrent.api.Context.OperatorContext;
import com.datatorrent.api.DAG;
import com.datatorrent.api.DAG.Locality;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.api.InputOperator;
import com.datatorrent.api.LocalMode;
import com.datatorrent.api.Operator.ActivationListener;

import com.datatorrent.common.util.Pair;

/**
 *
 */
public class KafkaExactlyOnceOutputOperatorTest extends KafkaOperatorTestBase
{
  private static final Logger logger = LoggerFactory.getLogger(KafkaExactlyOnceOutputOperatorTest.class);
  private static final int maxTuple = 40;
  private static CountDownLatch latch;
  private static boolean isRestarted = false;

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
        public void run()
        {
          try {
            int i = 0;
            while (dataGeneratorThread != null && i < maxTuple) {
              stringBuffer.put((++i) + "###testString " + i);
            }
            stringBuffer.put((maxTuple + 1) + "###" + KafkaOperatorTestBase.END_TUPLE);
          } catch (InterruptedException ie) {
            //
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
        if (i == 20 && isRestarted == false) {
          isRestarted = true;
          // fail the operator and when it gets back resend everything
          throw new RuntimeException();
        }
        outputPort.emit(stringBuffer.poll());
      }
    }
  } // End of StringGeneratorInputOperator

  /**
   * Test AbstractKafkaExactOnceOutputOperator (i.e. an output adapter for Kafka, aka producer).
   * This module sends data into a Kafka message bus.
   *
   * [Generate tuple] ==> [send tuple through Kafka output adapter(i.e. producer) into Kafka message bus](fail the producer at certain point and bring it back)
   * ==> [receive data in outside Kafka listener (i.e consumer)] ==> Verify kafka doesn't receive duplicated message
   *
   * @throws Exception
   */
  @Test
  @SuppressWarnings({"rawtypes"})
  public void testKafkaExactOnceOutputOperator() throws Exception
  {
    //initialize the latch to synchronize the threads
    latch = new CountDownLatch(maxTuple);
    // Setup a message listener to receive the message
    KafkaTestConsumer listener = new KafkaTestConsumer("topic1");
    listener.setLatch(latch);

    // Malhar module to send message
    // Create DAG for testing.
    LocalMode lma = LocalMode.newInstance();
    final DAG dag = lma.getDAG();

    StringGeneratorInputOperator generator = dag.addOperator("TestStringGenerator", StringGeneratorInputOperator.class);
    final SimpleKafkaExactOnceOutputOperator node = dag.addOperator("Kafka message producer", SimpleKafkaExactOnceOutputOperator.class);

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

    Future f = Executors.newFixedThreadPool(1).submit(listener);
    f.get(30, TimeUnit.SECONDS);

    lc.shutdown();

    // Check values send vs received
    Assert.assertEquals("Number of emitted tuples", maxTuple, listener.holdingBuffer.size());
    logger.debug(String.format("Number of emitted tuples: %d", listener.holdingBuffer.size()));
    Assert.assertEquals("First tuple", "testString 1", listener.getMessage(listener.holdingBuffer.peek()));

    listener.close();

  }

  public static class SimpleKafkaExactOnceOutputOperator extends AbstractExactlyOnceKafkaOutputOperator<String, String, String>
  {
    @Override
    protected int compareToLastMsg(Pair<String, String> tupleKeyValue, Pair<byte[], byte[]> lastReceivedKeyValue)
    {
      return Integer.parseInt(tupleKeyValue.first) - Integer.parseInt(new String(lastReceivedKeyValue.first));
    }

    @Override
    protected Pair<String, String> tupleToKeyValue(String tuple)
    {
      return new Pair<String, String>(tuple.split("###")[0], tuple.split("###")[1]);
    }

  }


}
