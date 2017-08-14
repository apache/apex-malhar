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
import java.util.HashMap;
import java.util.Map;
import java.util.Random;

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

import static org.apache.apex.malhar.lib.helper.OperatorContextTestHelper.mockOperatorContext;

import com.datatorrent.api.DAG;

/**
 * Test to verify JMS output operator adapter.
 */
public class JMSMultiPortOutputOperatorTest extends JMSTestBase
{
  private static final Logger logger = LoggerFactory.getLogger(JMSMultiPortOutputOperator.class);
  public  int tupleCount = 0;
  public static final transient int maxTuple = 20;

  public static final String CLIENT_ID = "Client1";
  public static final String APP_ID = "appId";
  public static final int OPERATOR_ID = 1;
  public JMSMultiPortOutputOperator outputOperator;
  public static OperatorContext testOperatorContext;
  public static final int HALF_BATCH_SIZE = 5;
  public static final int BATCH_SIZE = HALF_BATCH_SIZE * 2;
  public  final Random random = new Random();

  public static class TestMeta extends TestWatcher
  {
    @Override
    protected void starting(org.junit.runner.Description description)
    {
      logger.debug("Starting test {}", description.getMethodName());
      DefaultAttributeMap attributes = new DefaultAttributeMap();
      attributes.put(DAG.APPLICATION_ID, APP_ID);
      testOperatorContext = mockOperatorContext(OPERATOR_ID, attributes);

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

  @Test
  @SuppressWarnings("deprecation")
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
    byte[] byteArray = {0,1,2,3};

    while (i < maxTuple) {
      String tuple = "testString " + (++i);
      node.inputStringTypePort.process(tuple);
      node.inputObjectPort.process(i);
      Map<String,Integer> map = new HashMap<String, Integer>();
      map.put(tuple, i);
      node.inputMapPort.process(map);
      node.inputByteArrayPort.process(byteArray);
      tupleCount++;
    }

    node.endWindow();
    node.teardown();

    final long emittedCount = 80;

    Thread.sleep(1000);

    // Check values send vs received
    Assert.assertEquals("Number of emitted tuples", emittedCount, listener.receivedData.size());
    logger.debug(String.format("Number of emitted tuples: %d", listener.receivedData.size()));
    Assert.assertEquals("First tuple", "testString 1", listener.receivedData.get(1));
    Assert.assertEquals("Second Tuple",1, listener.receivedData.get(2));
    Map<String,Object> testmap = (HashMap<String,Object>)listener.receivedData.get(3);
    Map<String,Integer> map = new HashMap<String, Integer>();
    map.put("testString 1", 1);
    Assert.assertEquals("Third Tuple",map,testmap);
    Assert.assertArrayEquals("Fourth tuple",byteArray, (byte[])listener.receivedData.get(4));
    listener.closeConnection();
  }


}
