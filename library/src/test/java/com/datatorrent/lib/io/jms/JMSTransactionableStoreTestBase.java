/*
 *  Copyright (c) 2012-2014 Malhar, Inc.
 *  All Rights Reserved.
 */

package com.datatorrent.lib.io.jms;

import com.datatorrent.api.Attribute.AttributeMap.DefaultAttributeMap;
import com.datatorrent.api.DAG;
import com.datatorrent.lib.helper.OperatorContextTestHelper.TestIdOperatorContext;
import com.datatorrent.lib.io.jms.JMSOutputOperatorTest.JMSStringSinglePortOutputOperator;
import com.datatorrent.lib.util.ActiveMQMessageListener;
import java.io.File;
import javax.jms.JMSException;
import org.apache.commons.io.FileUtils;
import org.junit.*;
import org.junit.rules.TestWatcher;

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

  public static TestIdOperatorContext testOperatorContext;

  public static class TestMeta extends TestWatcher
  {
    @Override
    protected void starting(org.junit.runner.Description description)
    {
      //Create fresh operator context
      DefaultAttributeMap attributes = new DefaultAttributeMap();
      attributes.put(DAG.APPLICATION_ID, APP_ID);
      testOperatorContext = new TestIdOperatorContext(OPERATOR_ID, attributes);
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
    }
    catch (InstantiationException ex) {
      throw new RuntimeException(ex);
    }
    catch (IllegalAccessException ex) {
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

  //@Ignore
  @Test
  public void commitTest()
  {
    final ActiveMQMessageListener listener = new ActiveMQMessageListener();
    listener.setSubject(SUBJECT);

    try {
      listener.setupConnection();
    }
    catch (JMSException ex) {
      throw new RuntimeException(ex);
    }

    listener.run();

    createOperator();

    store.beginTransaction();
    outputOperator.inputPort.put("a");

    try {
      Thread.sleep(500);
    }
    catch (InterruptedException ex) {
      throw new RuntimeException(ex);
    }

    Assert.assertEquals(0, listener.receivedData.size());
    store.commitTransaction();

    try {
      Thread.sleep(500);
    }
    catch (InterruptedException ex) {
      throw new RuntimeException(ex);
    }

    Assert.assertEquals(1, listener.receivedData.size());

    deleteOperator();

    listener.closeConnection();
  }
}
