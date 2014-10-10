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
package com.datatorrent.lib.io.jms;

import javax.jms.JMSException;
import javax.jms.Message;

import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.api.annotation.InputPortFieldAnnotation;

import com.datatorrent.lib.util.ActiveMQMessageListener;

/**
 * Test to verify ActiveMQ output operator adapter.
 *
 */
public class ActiveMQOutputOperatorTest extends ActiveMQOperatorTestBase
{
  private static final Logger logger = LoggerFactory.getLogger(ActiveMQOutputOperatorTest.class);
  public static int tupleCount = 0;
  public static final transient int maxTuple = 20;

  /**
   * Concrete class of ActiveMQStringSinglePortOutputOperator for testing.
   */
  public static class ActiveMQStringSinglePortOutputOperator extends AbstractActiveMQSinglePortOutputOperator<String>
  {
    /**
     * Implementation of Abstract Method.
     *
     * @param tuple
     * @return
     */
    @Override
    protected Message createMessage(String tuple)
    {
      Message msg = null;
      try {
        msg = getSession().createTextMessage(tuple.toString());
      }
      catch (JMSException ex) {
        throw new RuntimeException("Failed to create message.", ex);
      }

      return msg;
    }
  } // End of ActiveMQStringSinglePortOutputOperator

  /**
   * Test AbstractActiveMQOutputOperator (i.e. an output adapter for ActiveMQ, aka producer).
   * This module sends data into an ActiveMQ message bus.
   *
   * [Generate tuple] ==> [send tuple through ActiveMQ output adapter(i.e. producer) into ActiveMQ message bus]
   * ==> [receive data in outside ActiveMQ listener]
   *
   * @throws Exception
   */
  @Test
  public void testActiveMQOutputOperator1() throws Exception
  {
    // Setup a message listener to receive the message
    final ActiveMQMessageListener listener = new ActiveMQMessageListener();
    listener.setupConnection();
    listener.run();

    // Malhar module to send message
    // Create ActiveMQStringSinglePortOutputOperator
    ActiveMQStringSinglePortOutputOperator node = new ActiveMQStringSinglePortOutputOperator();
    // Set configuration parameters for ActiveMQ
    node.setUser("");
    node.setPassword("");
    node.setUrl("tcp://localhost:61617");
    node.setAckMode("CLIENT_ACKNOWLEDGE");
    node.setClientId("Client1");
    node.setSubject("TEST.FOO");
    node.setMaximumSendMessages(15);
    node.setMessageSize(255);
    node.setBatch(10);
    node.setTopic(false);
    node.setDurable(false);
    node.setTransacted(false);
    node.setVerbose(true);

    node.setup(null);
    node.beginWindow(1);

    // produce data and process
    try {
      int i = 0;
      while (i < maxTuple) {
        String tuple = "testString " + (++i);
        node.inputPort.process(tuple);
        tupleCount++;
        Thread.sleep(20);
      }
    }
    catch (InterruptedException ie) {
    }
    node.endWindow();
    node.teardown();

    final long emittedCount = 15; //tupleCount < node.getMaximumSendMessages() ? tupleCount : node.getMaximumSendMessages();

    // Check values send vs received
    Assert.assertEquals("Number of emitted tuples", emittedCount, listener.receivedData.size());
    logger.debug(String.format("Number of emitted tuples: %d", listener.receivedData.size()));
    Assert.assertEquals("First tuple", "testString 1", listener.receivedData.get(new Integer(1)));

    listener.closeConnection();
  }

  /**
   * This test is same as prior one except maxMessage and topic setting is different.
   *
   * @throws Exception
   */
  @Test
  public void testActiveMQOutputOperator2() throws Exception
  {
    // Setup a message listener to receive the message
    final ActiveMQMessageListener listener = new ActiveMQMessageListener();
    listener.setTopic(true);
    listener.setupConnection();
    listener.run();

    // Create ActiveMQStringSinglePortOutputOperator
    ActiveMQStringSinglePortOutputOperator node = new ActiveMQStringSinglePortOutputOperator();
    // Set configuration parameters for ActiveMQ
    node.setUser("");
    node.setPassword("");
    node.setUrl("tcp://localhost:61617");
    node.setAckMode("CLIENT_ACKNOWLEDGE");
    node.setClientId("Client1");
    node.setSubject("TEST.FOO");
    node.setMaximumSendMessages(10); // topic setting is different than prior test
    node.setMessageSize(255);
    node.setBatch(10);
    node.setTopic(true); // topic setting is different than prior test
    node.setDurable(false);
    node.setTransacted(false);
    node.setVerbose(true);

    node.setup(null);
    node.beginWindow(1);

    // produce data and process
    try {
      int i = 0;
      while (i < maxTuple) {
        String tuple = "testString " + (++i);
        node.inputPort.process(tuple);
        tupleCount++;
        Thread.sleep(20);
      }
    }
    catch (InterruptedException ie) {
    }
    node.endWindow();
    node.teardown();

    final long emittedCount = tupleCount < node.getMaximumSendMessages() ? tupleCount : node.getMaximumSendMessages();

    // Check values send vs received
    Assert.assertEquals("Number of emitted tuples", emittedCount, listener.receivedData.size());
    logger.debug(String.format("Number of emitted tuples: %d", listener.receivedData.size()));
    Assert.assertEquals("First tuple", "testString 1", listener.receivedData.get(new Integer(1)));

    listener.closeConnection();
  }

  /**
   * Concrete class of ActiveMQStringSinglePortOutputOperator2 for testing.
   */
  public static class ActiveMQMultiPortOutputOperator extends AbstractActiveMQOutputOperator
  {
    /**
     * Two input ports.
     */
    @InputPortFieldAnnotation(name = "ActiveMQInputPort1")
    public final transient DefaultInputPort<String> inputPort1 = new DefaultInputPort<String>()
    {
      @Override
      public void process(String tuple)
      {
        try {
          Message msg = getSession().createTextMessage(tuple.toString());
          getProducer().send(msg);
          //logger.debug("process message {}", tuple.toString());
        }
        catch (JMSException ex) {
          throw new RuntimeException("Failed to create message.", ex);
        }
      }
    };
    @InputPortFieldAnnotation(name = "ActiveMQInputPort2")
    public final transient DefaultInputPort<Integer> inputPort2 = new DefaultInputPort<Integer>()
    {
      @Override
      public void process(Integer tuple)
      {
        try {
          Message msg = getSession().createTextMessage(tuple.toString());
          getProducer().send(msg);
          //logger.debug("process message {}", tuple.toString());
        }
        catch (JMSException ex) {
          throw new RuntimeException("Failed to create message.", ex);
        }
      }
    };
  } // End of ActiveMQMultiPortOutputOperator

  @Test
  public void testActiveMQMultiPortOutputOperator() throws Exception
  {
    // Setup a message listener to receive the message
    final ActiveMQMessageListener listener = new ActiveMQMessageListener();
    listener.setMaximumReceiveMessages(0);
    listener.setupConnection();
    listener.run();

    // Malhar module to send message
    ActiveMQMultiPortOutputOperator node = new ActiveMQMultiPortOutputOperator();
    // Set configuration parameters for ActiveMQ
    node.setUser("");
    node.setPassword("");
    node.setUrl("tcp://localhost:61617");
    node.setAckMode("CLIENT_ACKNOWLEDGE");
    node.setClientId("Client1");
    node.setSubject("TEST.FOO");
    node.setMaximumSendMessages(0);
    node.setMessageSize(255);
    node.setBatch(10);
    node.setTopic(false);
    node.setDurable(false);
    node.setTransacted(false);
    node.setVerbose(true);

    node.setup(null);
    node.beginWindow(1);

    // produce data and process
    try {
      int i = 0;
      while (i < maxTuple) {
        String tuple = "testString " + (++i);
        node.inputPort1.process(tuple);
        node.inputPort2.process(new Integer(i));
        tupleCount++;
        Thread.sleep(20);
      }
    }
    catch (InterruptedException ie) {
    }
    node.endWindow();
    node.teardown();

    final long emittedCount = 40;

    // Check values send vs received
    Assert.assertEquals("Number of emitted tuples", emittedCount, listener.receivedData.size());
    logger.debug(String.format("Number of emitted tuples: %d", listener.receivedData.size()));
    Assert.assertEquals("First tuple", "testString 1", listener.receivedData.get(new Integer(1)));

    listener.closeConnection();
  }
}
