/**
 * Copyright (C) 2015 DataTorrent, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.datatorrent.contrib.kinesis;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.conf.Configuration;

import com.datatorrent.api.*;
import com.datatorrent.api.DAG.Locality;

/**
 *
 */
@SuppressWarnings("rawtypes")
public abstract class KinesisOutputOperatorTest< O extends AbstractKinesisOutputOperator, G extends Operator > extends KinesisOperatorTestBase
{
  private static final Logger logger = LoggerFactory.getLogger(KinesisOutputOperatorTest.class);
  //protected static int tupleCount = 0;
  protected static final int maxTuple = 20;
  protected CountDownLatch doneLatch;

  private boolean enableConsumer = true;
  private Thread listenerThread;
  @Before
  public void beforeTest()
  {
    shardCount = 1;
    super.beforeTest();
  }

  
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
  @SuppressWarnings({"unchecked"})
  public void testKinesisOutputOperator() throws Exception
  {
    // Setup a message listener to receive the message
    KinesisTestConsumer listener = null;
    if( enableConsumer )
    {
      listener = createConsumerListener(streamName);
      if( listener != null )
      {
        //initialize the latch to synchronize the threads
        doneLatch = new CountDownLatch(maxTuple);
        listener.setDoneLatch(doneLatch);
        listenerThread = new Thread(listener);
        listenerThread.start();
      }
    }
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
    G generator = addGenerateOperator( dag );
    
    O node = addTestingOperator( dag );
    configureTestingOperator( node );

    // Connect ports
    dag.addStream("Kinesis message", getOutputPortOfGenerator( generator ), node.inputPort).setLocality(Locality.CONTAINER_LOCAL);

    Configuration conf = new Configuration(false);
    lma.prepareDAG(app, conf);

    // Create local cluster
    final LocalMode.Controller lc = lma.getController();
    lc.runAsync();

    int waitTime = 300000;
    if( doneLatch != null )
      doneLatch.await(waitTime, TimeUnit.MILLISECONDS);
    else
    {
      try
      {
        Thread.sleep(waitTime);
      }
      catch( Exception e ){}
    }
    
    if( listener != null )
      listener.setIsAlive(false);
    
    if( listenerThread != null )
      listenerThread.join( 1000 );
    
    lc.shutdown();

    // Check values send vs received
    if( listener != null )
    {
      Assert.assertEquals("Number of emitted tuples", maxTuple, listener.holdingBuffer.size());
      logger.debug(String.format("Number of emitted tuples: %d", listener.holdingBuffer.size()));
    }
    if( listener != null )
      listener.close();
  }

  protected KinesisTestConsumer createConsumerListener( String streamName )
  {
    KinesisTestConsumer listener = new KinesisTestConsumer(streamName);
    
    return listener;
  }
  
  protected void configureTestingOperator( O node )
  {
    node.setAccessKey(credentials.getCredentials().getAWSAccessKeyId());
    node.setSecretKey(credentials.getCredentials().getAWSSecretKey());
    node.setBatchSize(500);
    node.setStreamName(streamName);
  }
  
  protected abstract G addGenerateOperator( DAG dag );
  protected abstract DefaultOutputPort getOutputPortOfGenerator( G generator );
  protected abstract O addTestingOperator( DAG dag );

}