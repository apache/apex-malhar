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
package org.apache.apex.malhar.lib.io.jms;

import java.io.File;
import java.io.IOException;
import java.util.Random;

import javax.jms.JMSException;
import javax.jms.Message;

import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestWatcher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.apex.malhar.lib.util.ActiveMQMultiTypeMessageListener;
import org.apache.commons.io.FileUtils;

import com.datatorrent.api.Attribute.AttributeMap.DefaultAttributeMap;
import com.datatorrent.api.Context.OperatorContext;
import com.datatorrent.api.DAG;
import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.api.Operator.ProcessingMode;

import static org.apache.apex.malhar.lib.helper.OperatorContextTestHelper.mockOperatorContext;

/**
 * Test to verify JMS output operator adapter.
 */
public class JMSOutputOperatorTest extends JMSTestBase
{
  private static final Logger logger = LoggerFactory.getLogger(JMSOutputOperatorTest.class);
  public static int tupleCount = 0;
  public static final transient int maxTuple = 20;

  public static final String CLIENT_ID = "Client1";
  public static final String APP_ID = "appId";
  public static final int OPERATOR_ID = 1;
  public static JMSStringSinglePortOutputOperator outputOperator;
  public static OperatorContext testOperatorContext;
  public static OperatorContext testOperatorContextAMO;
  public static final int HALF_BATCH_SIZE = 5;
  public static final int BATCH_SIZE = HALF_BATCH_SIZE * 2;
  public static final Random random = new Random();

  public static class TestMeta extends TestWatcher
  {
    @Override
    protected void starting(org.junit.runner.Description description)
    {
      logger.debug("Starting test {}", description.getMethodName());
      DefaultAttributeMap attributes = new DefaultAttributeMap();
      attributes.put(DAG.APPLICATION_ID, APP_ID);
      testOperatorContext = mockOperatorContext(OPERATOR_ID, attributes);

      attributes = new DefaultAttributeMap();
      attributes.put(OperatorContext.PROCESSING_MODE, ProcessingMode.AT_MOST_ONCE);
      testOperatorContextAMO = mockOperatorContext(OPERATOR_ID, attributes);

      try {
        FileUtils.deleteDirectory(new File(FSPsuedoTransactionableStore.DEFAULT_RECOVERY_DIRECTORY));
      } catch (IOException ex) {
        throw new RuntimeException(ex);
      }
    }

    @Override
    protected void finished(org.junit.runner.Description description)
    {
      try {
        FileUtils.deleteDirectory(new File(FSPsuedoTransactionableStore.DEFAULT_RECOVERY_DIRECTORY));
      } catch (IOException ex) {
        throw new RuntimeException(ex);
      }
    }
  }

  @Rule
  public TestMeta testMeta = new TestMeta();

  /**
   * Concrete class of JMSStringSinglePortOutputOperator for testing.
   */
  public static class JMSStringSinglePortOutputOperator extends AbstractJMSSinglePortOutputOperator<String>
  {
    public JMSStringSinglePortOutputOperator()
    {
      this.setBatch(10);
    }

    @Override
    protected Message createMessage(Object tuple)
    {
      Message msg;
      try {
        msg = getSession().createTextMessage(tuple.toString());
      } catch (JMSException ex) {
        throw new RuntimeException("Failed to create message.", ex);
      }

      return msg;
    }
  } // End of JMSStringSinglePortOutputOperator

  /**
   * This is a helper method to create the output operator. Note this cannot be
   * put in test watcher because because JMS connection issues occur when this code
   * is run from a test watcher.
   */
  private void createOperator(boolean topic, OperatorContext context)
  {
    createOperator(topic, context, 0);
  }

  private void createOperator(boolean topic, OperatorContext context, @SuppressWarnings("unused") int maxMessages)
  {
    outputOperator = new JMSStringSinglePortOutputOperator();
    outputOperator.getConnectionFactoryProperties().put("userName", "");
    outputOperator.getConnectionFactoryProperties().put("password", "");
    outputOperator.getConnectionFactoryProperties().put("brokerURL", "tcp://localhost:61617");
    outputOperator.setAckMode("CLIENT_ACKNOWLEDGE");
    outputOperator.setClientId(CLIENT_ID);
    outputOperator.setSubject("TEST.FOO");
    outputOperator.setMessageSize(255);
    outputOperator.setBatch(BATCH_SIZE);
    outputOperator.setTopic(topic);
    outputOperator.setDurable(false);
    outputOperator.setVerbose(true);
    outputOperator.setup(context);
  }

  /**
   * Test AbstractJMSOutputOperator (i.e. an output adapter for JMS, aka producer).
   * This module sends data into an JMS message bus.
   *
   * [Generate tuple] ==> [send tuple through JMS output adapter(i.e. producer) into JMS message bus]
   * ==> [receive data in outside JMS listener]
   *
   * @throws Exception
   */
  //@Ignore
  @Test
  public void testJMSOutputOperator1() throws Exception
  {
    // Setup a message listener to receive the message
    final ActiveMQMultiTypeMessageListener listener = new ActiveMQMultiTypeMessageListener();
    listener.setupConnection();
    listener.run();

    createOperator(false, testOperatorContext, 15);

    outputOperator.beginWindow(1);

    int i = 0;
    while (i < maxTuple) {
      logger.debug("Emitting tuple {}", i);
      String tuple = "testString " + (++i);
      outputOperator.inputPort.process(tuple);
      tupleCount++;
    }

    outputOperator.endWindow();
    outputOperator.teardown();

    Thread.sleep(500);

    // Check values send vs received
    Assert.assertEquals("Number of emitted tuples", maxTuple, listener.receivedData.size());
    logger.debug(String.format("Number of emitted tuples: %d", listener.receivedData.size()));
    Assert.assertEquals("First tuple", "testString 1", listener.receivedData.get(1));

    listener.closeConnection();
  }

  /**
   * This test is same as prior one except maxMessage and topic setting is different.
   *
   * @throws Exception
   */
  ////@Ignore
  @Test
  public void testJMSOutputOperator2() throws Exception
  {
    // Setup a message listener to receive the message
    final ActiveMQMultiTypeMessageListener listener = new ActiveMQMultiTypeMessageListener();
    listener.setTopic(true);
    listener.setupConnection();
    listener.run();

    createOperator(true, testOperatorContext, 15);

    outputOperator.beginWindow(1);

    // produce data and process

    int i = 0;
    while (i < maxTuple) {
      String tuple = "testString " + (++i);
      outputOperator.inputPort.process(tuple);
      tupleCount++;
    }

    outputOperator.endWindow();
    outputOperator.teardown();

    Thread.sleep(500);

    // Check values send vs received
    Assert.assertEquals("Number of emitted tuples", maxTuple, listener.receivedData.size());
    logger.debug(String.format("Number of emitted tuples: %d", listener.receivedData.size()));
    Assert.assertEquals("First tuple", "testString 1", listener.receivedData.get(1));

    listener.closeConnection();
  }

  //@Ignore
  @Test
  public void testBatch() throws JMSException, InterruptedException
  {
    // Setup a message listener to receive the message
    final ActiveMQMultiTypeMessageListener listener = new ActiveMQMultiTypeMessageListener();
    listener.setTopic(false);

    listener.setupConnection();
    listener.run();

    createOperator(false, testOperatorContext);

    outputOperator.beginWindow(0);

    for (int batchCounter = 0; batchCounter < BATCH_SIZE; batchCounter++) {
      outputOperator.inputPort.put(Integer.toString(random.nextInt()));
    }

    outputOperator.endWindow();
    Thread.sleep(200);

    Assert.assertEquals("Batch should be written", BATCH_SIZE, listener.receivedData.size());

    outputOperator.beginWindow(1);

    for (int batchCounter = 0; batchCounter < HALF_BATCH_SIZE; batchCounter++) {
      outputOperator.inputPort.put(Integer.toString(random.nextInt()));
    }

    outputOperator.endWindow();
    Thread.sleep(200);

    Assert.assertEquals("Batch should not be written", BATCH_SIZE + HALF_BATCH_SIZE, listener.receivedData.size());

    outputOperator.beginWindow(2);

    for (int batchCounter = 0; batchCounter < HALF_BATCH_SIZE; batchCounter++) {
      outputOperator.inputPort.put(Integer.toString(random.nextInt()));
    }

    outputOperator.endWindow();
    Thread.sleep(200);

    Assert.assertEquals("Batch should not be written", 2 * BATCH_SIZE, listener.receivedData.size());

    outputOperator.teardown();
    listener.closeConnection();
  }

  @Test
  public void testAtLeastOnceFullBatch() throws JMSException, InterruptedException
  {
    // Setup a message listener to receive the message
    final ActiveMQMultiTypeMessageListener listener = new ActiveMQMultiTypeMessageListener();
    listener.setTopic(false);

    listener.setupConnection();
    listener.run();

    createOperator(false, testOperatorContext);

    outputOperator.beginWindow(0);

    for (int batchCounter = 0; batchCounter < BATCH_SIZE; batchCounter++) {
      outputOperator.inputPort.put(Integer.toString(random.nextInt()));
    }

    outputOperator.endWindow();

    Thread.sleep(200);

    Assert.assertEquals("Batch should be written", BATCH_SIZE, listener.receivedData.size());

    outputOperator.beginWindow(1);

    for (int batchCounter = 0; batchCounter < BATCH_SIZE; batchCounter++) {
      outputOperator.inputPort.put(Integer.toString(random.nextInt()));
    }

    Thread.sleep(200);

    Assert.assertEquals("Batch should be written", BATCH_SIZE, listener.receivedData.size());

    outputOperator.teardown();

    ////

    outputOperator.setup(testOperatorContext);

    Thread.sleep(200);
    Assert.assertEquals("Batch should be written", BATCH_SIZE, listener.receivedData.size());

    outputOperator.beginWindow(0);

    for (int batchCounter = 0; batchCounter < BATCH_SIZE; batchCounter++) {
      outputOperator.inputPort.put(Integer.toString(random.nextInt()));
    }

    outputOperator.endWindow();

    Thread.sleep(200);
    Assert.assertEquals("Batch should be written", BATCH_SIZE, listener.receivedData.size());

    outputOperator.beginWindow(1);

    for (int batchCounter = 0; batchCounter < BATCH_SIZE; batchCounter++) {
      outputOperator.inputPort.put(Integer.toString(random.nextInt()));
    }

    outputOperator.endWindow();

    Thread.sleep(200);
    Assert.assertEquals("Batch should be written", 2 * BATCH_SIZE, listener.receivedData.size());

    listener.closeConnection();
  }

  //@Ignore
  @Test
  public void testAtLeastOnceHalfBatch() throws JMSException, InterruptedException
  {
    // Setup a message listener to receive the message
    final ActiveMQMultiTypeMessageListener listener = new ActiveMQMultiTypeMessageListener();
    listener.setTopic(false);

    listener.setupConnection();
    listener.run();

    createOperator(false, testOperatorContext);

    outputOperator.beginWindow(0);

    for (int batchCounter = 0; batchCounter < BATCH_SIZE; batchCounter++) {
      outputOperator.inputPort.put(Integer.toString(random.nextInt()));
    }

    outputOperator.endWindow();

    Thread.sleep(200);
    Assert.assertEquals("Batch should be written", BATCH_SIZE, listener.receivedData.size());

    outputOperator.beginWindow(1);
    for (int batchCounter = 0; batchCounter < HALF_BATCH_SIZE; batchCounter++) {
      outputOperator.inputPort.put(Integer.toString(random.nextInt()));
    }

    Thread.sleep(200);
    Assert.assertEquals("Batch should be written", BATCH_SIZE, listener.receivedData.size());

    outputOperator.teardown();

    ////

    outputOperator.setup(testOperatorContext);

    Thread.sleep(200);
    Assert.assertEquals("Batch should be written", BATCH_SIZE, listener.receivedData.size());

    outputOperator.beginWindow(0);

    for (int batchCounter = 0; batchCounter < BATCH_SIZE; batchCounter++) {
      outputOperator.inputPort.put(Integer.toString(random.nextInt()));
    }

    outputOperator.endWindow();

    Thread.sleep(200);
    Assert.assertEquals("Batch should be written", BATCH_SIZE, listener.receivedData.size());

    outputOperator.beginWindow(1);
    for (int batchCounter = 0; batchCounter < HALF_BATCH_SIZE; batchCounter++) {
      outputOperator.inputPort.put(Integer.toString(random.nextInt()));
    }

    outputOperator.endWindow();
    Thread.sleep(200);
    Assert.assertEquals("Batch should be written", BATCH_SIZE + HALF_BATCH_SIZE, listener.receivedData.size());
    listener.closeConnection();
  }

  //@Ignore
  @Test
  public void testAtMostOnceFullBatch()
  {
    // Setup a message listener to receive the message
    final ActiveMQMultiTypeMessageListener listener = new ActiveMQMultiTypeMessageListener();
    listener.setTopic(false);

    try {
      listener.setupConnection();
    } catch (JMSException ex) {
      throw new RuntimeException(ex);
    }

    listener.run();

    createOperator(false, testOperatorContextAMO);

    outputOperator.beginWindow(0);

    for (int batchCounter = 0; batchCounter < BATCH_SIZE; batchCounter++) {
      outputOperator.inputPort.put(Integer.toString(random.nextInt()));
    }

    outputOperator.endWindow();

    try {
      Thread.sleep(200);
    } catch (InterruptedException ex) {
      throw new RuntimeException(ex);
    }
    Assert.assertEquals("Batch should be written", BATCH_SIZE, listener.receivedData.size());

    outputOperator.beginWindow(1);

    for (int batchCounter = 0; batchCounter < BATCH_SIZE; batchCounter++) {
      outputOperator.inputPort.put(Integer.toString(random.nextInt()));
    }

    try {
      Thread.sleep(200);
    } catch (InterruptedException ex) {
      throw new RuntimeException(ex);
    }
    Assert.assertEquals("Batch should be written", BATCH_SIZE, listener.receivedData.size());

    outputOperator.teardown();

    ////

    outputOperator.setup(testOperatorContext);

    outputOperator.beginWindow(2);

    for (int batchCounter = 0; batchCounter < BATCH_SIZE; batchCounter++) {
      outputOperator.inputPort.put(Integer.toString(random.nextInt()));
    }

    outputOperator.endWindow();

    try {
      Thread.sleep(200);
    } catch (InterruptedException ex) {
      throw new RuntimeException(ex);
    }
    Assert.assertEquals("Batch should be written", 2 * BATCH_SIZE, listener.receivedData.size());

    listener.closeConnection();
  }

  //@Ignore
  @Test
  public void testAtMostOnceHalfBatch()
  {
    // Setup a message listener to receive the message
    final ActiveMQMultiTypeMessageListener listener = new ActiveMQMultiTypeMessageListener();
    listener.setTopic(false);

    try {
      listener.setupConnection();
    } catch (JMSException ex) {
      throw new RuntimeException(ex);
    }

    listener.run();

    createOperator(false, testOperatorContextAMO);

    outputOperator.beginWindow(0);

    for (int batchCounter = 0; batchCounter < BATCH_SIZE; batchCounter++) {
      outputOperator.inputPort.put(Integer.toString(random.nextInt()));
    }

    outputOperator.endWindow();

    try {
      Thread.sleep(200);
    } catch (InterruptedException ex) {
      throw new RuntimeException(ex);
    }
    Assert.assertEquals("Batch should be written", BATCH_SIZE, listener.receivedData.size());

    outputOperator.beginWindow(1);

    for (int batchCounter = 0; batchCounter < HALF_BATCH_SIZE; batchCounter++) {
      outputOperator.inputPort.put(Integer.toString(random.nextInt()));
    }

    try {
      Thread.sleep(200);
    } catch (InterruptedException ex) {
      throw new RuntimeException(ex);
    }
    Assert.assertEquals("Batch should be written", BATCH_SIZE, listener.receivedData.size());

    outputOperator.teardown();

    ////

    outputOperator.setup(testOperatorContext);

    try {
      Thread.sleep(200);
    } catch (InterruptedException ex) {
      throw new RuntimeException(ex);
    }
    Assert.assertEquals("Batch should be written", BATCH_SIZE, listener.receivedData.size());

    outputOperator.beginWindow(2);

    for (int batchCounter = 0; batchCounter < BATCH_SIZE; batchCounter++) {
      outputOperator.inputPort.put(Integer.toString(random.nextInt()));
    }

    outputOperator.endWindow();

    try {
      Thread.sleep(200);
    } catch (InterruptedException ex) {
      throw new RuntimeException(ex);
    }
    Assert.assertEquals("Batch should be written", 2 * BATCH_SIZE, listener.receivedData.size());
    listener.closeConnection();
  }

  /**
   * Concrete class of JMSMultiPortOutputOperator for testing.
   */
  public static class JMSMultiPortOutputOperator extends AbstractJMSOutputOperator
  {
    /**
     * Two input ports.
     */
    public final transient DefaultInputPort<String> inputPort1 = new DefaultInputPort<String>()
    {
      @Override
      public void process(String tuple)
      {
        sendMessage(tuple);
      }
    };
    public final transient DefaultInputPort<Integer> inputPort2 = new DefaultInputPort<Integer>()
    {
      @Override
      public void process(Integer tuple)
      {
        sendMessage(tuple);
      }
    };

    @Override
    protected Message createMessage(Object tuple)
    {
      Message msg;

      try {
        msg = getSession().createTextMessage(tuple.toString());
      } catch (JMSException ex) {
        throw new RuntimeException(ex);
      }

      return msg;
    }
  }

  @Test
  public void testJMSMultiPortOutputOperator() throws Exception
  {
    // Setup a message listener to receive the message
    final ActiveMQMultiTypeMessageListener listener = new ActiveMQMultiTypeMessageListener();
    listener.setMaximumReceiveMessages(0);
    listener.setupConnection();
    listener.run();

    // Malhar module to send message
    JMSMultiPortOutputOperator node = new JMSMultiPortOutputOperator();
    // Set configuration parameters for JMS

    node.getConnectionFactoryProperties().put("userName", "");
    node.getConnectionFactoryProperties().put("password", "");
    node.getConnectionFactoryProperties().put("brokerURL", "tcp://localhost:61617");
    node.setAckMode("CLIENT_ACKNOWLEDGE");
    node.setClientId("Client1");
    node.setSubject("TEST.FOO");
    node.setMessageSize(255);
    node.setBatch(10);
    node.setTopic(false);
    node.setDurable(false);
    logger.debug("Before set store");
    node.setStore(new JMSTransactionableStore());
    logger.debug("After set store");
    logger.debug("Store class {}", node.getStore());
    node.setVerbose(true);

    node.setup(testOperatorContext);
    node.beginWindow(1);

    int i = 0;
    while (i < maxTuple) {
      String tuple = "testString " + (++i);
      node.inputPort1.process(tuple);
      node.inputPort2.process(i);
      tupleCount++;
    }

    node.endWindow();
    node.teardown();

    final long emittedCount = 40;

    Thread.sleep(500);

    // Check values send vs received
    Assert.assertEquals("Number of emitted tuples", emittedCount, listener.receivedData.size());
    logger.debug(String.format("Number of emitted tuples: %d", listener.receivedData.size()));
    Assert.assertEquals("First tuple", "testString 1", listener.receivedData.get(1));

    listener.closeConnection();
  }
}
