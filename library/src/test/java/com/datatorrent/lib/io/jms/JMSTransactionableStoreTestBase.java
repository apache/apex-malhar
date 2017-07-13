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
package com.datatorrent.lib.io.jms;

import java.io.File;

import javax.jms.JMSException;

import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestWatcher;

import org.apache.commons.io.FileUtils;

import com.datatorrent.api.Attribute.AttributeMap.DefaultAttributeMap;
import com.datatorrent.api.Context.OperatorContext;
import com.datatorrent.api.DAG;
import com.datatorrent.lib.io.jms.JMSOutputOperatorTest.JMSStringSinglePortOutputOperator;
import com.datatorrent.lib.util.ActiveMQMultiTypeMessageListener;

import static com.datatorrent.lib.helper.OperatorContextTestHelper.mockOperatorContext;

/**
 * Base testing class for testing transactionable store implementations.
 */
public class JMSTransactionableStoreTestBase extends JMSTestBase
{
  public static final String SUBJECT = "TEST.FOO";
  public static final String CLIENT_ID = "Client1";
  public static final String APP_ID = "appId";
  public static final int OPERATOR_ID = 1;
  public static final int OPERATOR_2_ID = 2;
  public static Class<? extends JMSBaseTransactionableStore> storeClass;

  public static OperatorContext testOperatorContext;
  public static OperatorContext testOperator2Context;

  public static class TestMeta extends TestWatcher
  {
    @Override
    protected void starting(org.junit.runner.Description description)
    {
      //Create fresh operator context
      DefaultAttributeMap attributes = new DefaultAttributeMap();
      attributes.put(DAG.APPLICATION_ID, APP_ID);
      testOperatorContext = mockOperatorContext(OPERATOR_ID, attributes);
      testOperator2Context = mockOperatorContext(OPERATOR_2_ID, attributes);
      FileUtils.deleteQuietly(new File(FSPsuedoTransactionableStore.DEFAULT_RECOVERY_DIRECTORY));
    }

    @Override
    protected void finished(org.junit.runner.Description description)
    {
      FileUtils.deleteQuietly(new File(FSPsuedoTransactionableStore.DEFAULT_RECOVERY_DIRECTORY));
    }
  }

  @Rule
  public TestMeta testMeta = new TestMeta();

  public JMSTransactionableStoreTestBase(Class<? extends JMSBaseTransactionableStore> storeClass)
  {
    JMSTransactionableStoreTestBase.storeClass = storeClass;
  }

  /**
   * This is a helper method to create the output operator. Note this cannot be
   * put in test watcher because because JMS connection issues occur when this code
   * is run from a test watcher.
   *
   * @param metaQueueName metaQueueName to set in JMSTransactionableStore
   */
  private JMSStringSinglePortOutputOperator createOperator(OperatorContext context, String metaQueueName)
  {
    JMSStringSinglePortOutputOperator outputOperator = new JMSStringSinglePortOutputOperator();
    JMSBaseTransactionableStore store;
    try {
      store = storeClass.newInstance();
    } catch (InstantiationException | IllegalAccessException ex) {
      throw new RuntimeException(ex);
    }

    if (JMSTransactionableStore.class.equals(storeClass) && metaQueueName != null) {
      ((JMSTransactionableStore)store).setMetaQueueName(metaQueueName);
    }
    outputOperator.getConnectionFactoryProperties().put("userName", "");
    outputOperator.getConnectionFactoryProperties().put("password", "");
    outputOperator.getConnectionFactoryProperties().put("brokerURL", "tcp://localhost:61617");
    outputOperator.setAckMode("CLIENT_ACKNOWLEDGE");
    outputOperator.setClientId(CLIENT_ID);
    outputOperator.setSubject("TEST.FOO");
    outputOperator.setMessageSize(255);
    outputOperator.setBatch(1);
    outputOperator.setTopic(false);
    outputOperator.setDurable(false);
    outputOperator.setStore(store);
    outputOperator.setVerbose(true);
    outputOperator.setup(context);

    return outputOperator;
  }

  @Test
  public void connectedTest()
  {
    JMSStringSinglePortOutputOperator jmsOutputOperator = createOperator(testOperatorContext, null);
    JMSBaseTransactionableStore store = jmsOutputOperator.getStore();

    Assert.assertTrue("Should be connected.", store.isConnected());
    jmsOutputOperator.teardown();
    Assert.assertFalse("Should not be connected.", store.isConnected());
  }

  @Test
  public void transactionTest()
  {
    JMSStringSinglePortOutputOperator jmsOutputOperator = createOperator(testOperatorContext, null);
    JMSBaseTransactionableStore store = jmsOutputOperator.getStore();

    Assert.assertFalse("Should not be in transaction.", store.isInTransaction());
    store.beginTransaction();
    Assert.assertTrue("Should be in transaction.", store.isInTransaction());
    store.commitTransaction();
    Assert.assertFalse("Should not be in transaction.", store.isInTransaction());

    jmsOutputOperator.teardown();
  }

  @Test
  public void storeRetreiveTransactionTest()
  {
    JMSStringSinglePortOutputOperator jmsOutputOperator = createOperator(testOperatorContext, null);
    JMSBaseTransactionableStore store = jmsOutputOperator.getStore();

    jmsOutputOperator.beginWindow(0L);
    jmsOutputOperator.endWindow();

    long windowId = store.getCommittedWindowId(APP_ID, OPERATOR_ID);
    Assert.assertEquals(0L, windowId);

    jmsOutputOperator.teardown();
  }

  /**
   * Creates two operators with different operatorId and same appId to test correct functionality of storing and
   * retrieving windowId with message selector
   */
  @Test
  public void twoOperatorsStoreRetrieveWithMessageSelectorTransactionTest()
  {
    JMSStringSinglePortOutputOperator jmsOutputOperator = createOperator(testOperatorContext, null);

    jmsOutputOperator.beginWindow(0L);
    jmsOutputOperator.endWindow();

    //Create fresh operator context

    JMSStringSinglePortOutputOperator jmsOutputOperator2 = createOperator(testOperator2Context, null);
    jmsOutputOperator2.beginWindow(1L);
    jmsOutputOperator2.endWindow();

    long windowIdOp = jmsOutputOperator.getStore().getCommittedWindowId(APP_ID, OPERATOR_ID);
    Assert.assertEquals(0L, windowIdOp);

    long windowIdOp2 = jmsOutputOperator2.getStore().getCommittedWindowId(APP_ID, OPERATOR_2_ID);
    Assert.assertEquals(1L, windowIdOp2);

    jmsOutputOperator.teardown();
    jmsOutputOperator2.teardown();
  }

  /**
   * Similar to the test above with using a custom metaQueueName
   */
  @Test
  public void twoOperatorsStoreRetrieveWithMessageSelectorTransactionTestWithCustomMetaQueueName()
  {
    JMSStringSinglePortOutputOperator jmsOutputOperator = createOperator(testOperatorContext, "metaQ1");

    jmsOutputOperator.beginWindow(0L);
    jmsOutputOperator.endWindow();

    //Create fresh operator context

    JMSStringSinglePortOutputOperator jmsOutputOperator2 = createOperator(testOperator2Context, "metaQ2");
    jmsOutputOperator2.beginWindow(1L);
    jmsOutputOperator2.endWindow();

    long windowIdOp = jmsOutputOperator.getStore().getCommittedWindowId(APP_ID, OPERATOR_ID);
    Assert.assertEquals(0L, windowIdOp);

    long windowIdOp2 = jmsOutputOperator2.getStore().getCommittedWindowId(APP_ID, OPERATOR_2_ID);
    Assert.assertEquals(1L, windowIdOp2);

    jmsOutputOperator.teardown();
    jmsOutputOperator2.teardown();
  }

  @Test
  public void multiWindowTransactionTest()
  {
    JMSStringSinglePortOutputOperator jmsOutputOperator = createOperator(testOperatorContext, null);
    JMSBaseTransactionableStore store = jmsOutputOperator.getStore();

    long windowId = store.getCommittedWindowId(APP_ID, OPERATOR_ID);
    Assert.assertEquals(-1L, windowId);

    jmsOutputOperator.beginWindow(0L);
    jmsOutputOperator.endWindow();

    windowId = store.getCommittedWindowId(APP_ID, OPERATOR_ID);
    Assert.assertEquals(0L, windowId);

    jmsOutputOperator.beginWindow(1L);
    jmsOutputOperator.endWindow();

    windowId = store.getCommittedWindowId(APP_ID, OPERATOR_ID);
    Assert.assertEquals(1L, windowId);

    jmsOutputOperator.beginWindow(2L);
    jmsOutputOperator.endWindow();

    windowId = store.getCommittedWindowId(APP_ID, OPERATOR_ID);
    Assert.assertEquals(2L, windowId);

    jmsOutputOperator.beginWindow(3L);
    jmsOutputOperator.endWindow();

    windowId = store.getCommittedWindowId(APP_ID, OPERATOR_ID);
    Assert.assertEquals(3L, windowId);

    jmsOutputOperator.beginWindow(4L);
    jmsOutputOperator.endWindow();

    jmsOutputOperator.teardown();
  }

  @Test
  public void commitTest() throws JMSException, InterruptedException
  {
    final ActiveMQMultiTypeMessageListener listener = new ActiveMQMultiTypeMessageListener();
    listener.setSubject(SUBJECT);

    listener.setupConnection();
    listener.run();

    JMSStringSinglePortOutputOperator jmsOutputOperator = createOperator(testOperatorContext, null);
    JMSBaseTransactionableStore store = jmsOutputOperator.getStore();

    store.beginTransaction();
    jmsOutputOperator.inputPort.put("a");

    Thread.sleep(500);
    Assert.assertEquals(0, listener.receivedData.size());
    store.commitTransaction();

    Thread.sleep(500);
    Assert.assertEquals(1, listener.receivedData.size());

    jmsOutputOperator.teardown();

    listener.closeConnection();
  }
}
