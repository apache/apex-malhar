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
import com.datatorrent.api.DAG;
import com.datatorrent.lib.helper.OperatorContextTestHelper.MockOperatorContext;
import com.datatorrent.lib.io.jms.JMSOutputOperatorTest.JMSStringSinglePortOutputOperator;
import com.datatorrent.lib.util.ActiveMQMultiTypeMessageListener;

/**
 * Base testing class for testing transactionable store implementations.
 */
public class JMSTransactionableStoreTestBase extends JMSTestBase
{
  public static final String SUBJECT = "TEST.FOO";
  public static final String CLIENT_ID = "Client1";
  public static final String APP_ID = "appId";
  public static final int OPERATOR_ID = 1;
  public static JMSBaseTransactionableStore store;
  public static JMSStringSinglePortOutputOperator outputOperator;
  public static Class<? extends JMSBaseTransactionableStore> storeClass;

  public static MockOperatorContext testOperatorContext;

  public static class TestMeta extends TestWatcher
  {
    @Override
    protected void starting(org.junit.runner.Description description)
    {
      //Create fresh operator context
      DefaultAttributeMap attributes = new DefaultAttributeMap();
      attributes.put(DAG.APPLICATION_ID, APP_ID);
      testOperatorContext = MockOperatorContext.of(OPERATOR_ID, attributes);
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
   */
  private void createOperator()
  {
    outputOperator = new JMSStringSinglePortOutputOperator();

    try {
      store = storeClass.newInstance();
    } catch (InstantiationException | IllegalAccessException ex) {
      throw new RuntimeException(ex);
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
    outputOperator.setup(testOperatorContext);
  }

  /**
   * This is a helper method to teardown a test operator.
   */
  private void deleteOperator()
  {
    outputOperator.teardown();
  }

  //@Ignore
  @Test
  public void connectedTest()
  {
    createOperator();
    Assert.assertTrue("Should be connected.", store.isConnected());
    deleteOperator();
    Assert.assertFalse("Should not be connected.", store.isConnected());
  }

  //@Ignore
  @Test
  public void transactionTest()
  {
    createOperator();

    Assert.assertFalse("Should not be in transaction.", store.isInTransaction());
    store.beginTransaction();
    Assert.assertTrue("Should be in transaction.", store.isInTransaction());
    store.commitTransaction();
    Assert.assertFalse("Should not be in transaction.", store.isInTransaction());

    deleteOperator();
  }

  //@Ignore
  @Test
  public void storeRetreiveTransactionTest()
  {
    createOperator();

    outputOperator.beginWindow(0L);
    outputOperator.endWindow();

    long windowId = store.getCommittedWindowId(APP_ID, OPERATOR_ID);
    Assert.assertEquals(0L, windowId);

    deleteOperator();
  }

  ////@Ignore
  @Test
  public void multiWindowTransactionTest()
  {
    createOperator();

    long windowId = store.getCommittedWindowId(APP_ID, OPERATOR_ID);
    Assert.assertEquals(-1L, windowId);

    outputOperator.beginWindow(0L);
    outputOperator.endWindow();

    windowId = store.getCommittedWindowId(APP_ID, OPERATOR_ID);
    Assert.assertEquals(0L, windowId);

    outputOperator.beginWindow(1L);
    outputOperator.endWindow();

    windowId = store.getCommittedWindowId(APP_ID, OPERATOR_ID);
    Assert.assertEquals(1L, windowId);

    outputOperator.beginWindow(2L);
    outputOperator.endWindow();

    windowId = store.getCommittedWindowId(APP_ID, OPERATOR_ID);
    Assert.assertEquals(2L, windowId);

    outputOperator.beginWindow(3L);
    outputOperator.endWindow();

    windowId = store.getCommittedWindowId(APP_ID, OPERATOR_ID);
    Assert.assertEquals(3L, windowId);

    outputOperator.beginWindow(4L);
    outputOperator.endWindow();

    deleteOperator();
  }

  @Test
  public void commitTest() throws JMSException, InterruptedException
  {
    final ActiveMQMultiTypeMessageListener listener = new ActiveMQMultiTypeMessageListener();
    listener.setSubject(SUBJECT);

    listener.setupConnection();
    listener.run();

    createOperator();

    store.beginTransaction();
    outputOperator.inputPort.put("a");

    Thread.sleep(500);
    Assert.assertEquals(0, listener.receivedData.size());
    store.commitTransaction();

    Thread.sleep(500);
    Assert.assertEquals(1, listener.receivedData.size());

    deleteOperator();

    listener.closeConnection();
  }
}
