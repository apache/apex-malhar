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
package com.datatorrent.contrib.kinesis;

import com.datatorrent.api.Context.OperatorContext;
import com.datatorrent.api.*;
import com.datatorrent.api.DAG.Locality;
import com.datatorrent.api.Operator.ActivationListener;
import org.apache.hadoop.conf.Configuration;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

/**
 *
 */
public class KinesisOutputOperatorTest extends KinesisOperatorTestBase
{
  private static final Logger logger = LoggerFactory.getLogger(KinesisOutputOperatorTest.class);
  private static int tupleCount = 0;
  private static final int maxTuple = 20;
  private static CountDownLatch latch;

  @Before
  public void beforeTest()
  {
    shardCount = 1;
    super.beforeTest();
  }

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
            stringBuffer.put(KinesisOperatorTestBase.END_TUPLE);
          }
          catch (Exception ie) {
            throw new RuntimeException(ie);
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
   * Test AbstractKinesisOutputOperator (i.e. an output adapter for Kinesis, aka producer).
   * This module sends data into an ActiveMQ message bus.
   *
   * [Generate tuple] ==> [send tuple through Kinesis output adapter(i.e. producer) into Kinesis message bus]
   * ==> [receive data in outside Kinesis listener (i.e consumer)]
   *
   * @throws Exception
   */
  @Test
  @SuppressWarnings({"SleepWhileInLoop", "empty-statement", "rawtypes"})
  public void testKinesieOutputOperator() throws Exception
  {
    //initialize the latch to synchronize the threads
    latch = new CountDownLatch(maxTuple);
    // Setup a message listener to receive the message
    KinesisTestConsumer listener = new KinesisTestConsumer(streamName);
    listener.setLatch(latch);
    new Thread(listener).start();

    // Create DAG for testing.
    LocalMode lma = LocalMode.newInstance();

    StreamingApplication app = new StreamingApplication() {
      @Override
      public void populateDAG(DAG dag, Configuration conf)
      {
      }
    };

    DAG dag = lma.getDAG();

    // Create ActiveMQStringSinglePortOutputOperator
    StringGeneratorInputOperator generator = dag.addOperator("TestStringGenerator", StringGeneratorInputOperator.class);
    KinesisStringOutputOperator node = dag.addOperator("KinesisMessageProducer", KinesisStringOutputOperator.class);
    node.setAccessKey(credentials.getCredentials().getAWSSecretKey());
    node.setSecretKey(credentials.getCredentials().getAWSAccessKeyId());
    node.setBatchSize(500);

    node.setStreamName(streamName);

    // Connect ports
    dag.addStream("Kinesis message", generator.outputPort, node.inputPort).setLocality(Locality.CONTAINER_LOCAL);

    Configuration conf = new Configuration(false);
    lma.prepareDAG(app, conf);

    // Create local cluster
    final LocalMode.Controller lc = lma.getController();
    lc.runAsync();

    // Immediately return unless latch timeout in 5 seconds
    latch.await(15, TimeUnit.SECONDS);
    lc.shutdown();

    // Check values send vs received
    Assert.assertEquals("Number of emitted tuples", tupleCount, listener.holdingBuffer.size());
    logger.debug(String.format("Number of emitted tuples: %d", listener.holdingBuffer.size()));

    listener.close();
  }


}