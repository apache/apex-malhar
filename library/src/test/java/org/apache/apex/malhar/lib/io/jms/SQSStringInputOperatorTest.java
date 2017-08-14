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

import javax.jms.ConnectionFactory;

import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.internal.AssumptionViolatedException;
import org.junit.rules.TestWatcher;
import org.junit.runner.Description;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.apex.malhar.lib.testbench.CollectorTestSink;
import org.apache.commons.io.FileUtils;

import com.amazon.sqs.javamessaging.SQSConnectionFactory;
import com.amazonaws.auth.PropertiesFileCredentialsProvider;
import com.amazonaws.regions.Region;
import com.amazonaws.regions.Regions;
import com.datatorrent.api.Attribute;
import com.datatorrent.api.Context;

import static org.apache.apex.malhar.lib.helper.OperatorContextTestHelper.mockOperatorContext;

/**
 * Tests for {@link JMSStringInputOperator} for AMZ SQS.
 * Note: for SQS we should use AckMode as "AUTO_ACKNOWLEDGE" and
 * no transacted mode (transacted = false).
 *
 * Note: check the comment for com.amazon.sqs.javamessaging.SQSMessageConsumer.close()
 * specifically: "Since consumer prefetch threads use SQS long-poll feature with 20 seconds
     * timeout, closing each consumer prefetch thread can take up to 20 seconds,
     * which in-turn will impact the time on consumer close."
 *
 * Because of the above this test takes a long time due to consumer.close() in
 * org.apache.apex.malhar.lib.io.jms.AbstractJMSInputOperator.cleanup()
 *
 * NOTE: tests are automatically skipped if the secret key in sqstestCreds.properties
 * is missing or blank.
 *
 * NOTE: each test creates its own uniquely named queue in SQS and then deletes it afterwards.
 * Also we try to scrub any leftover queues from the previous runs just in case tests were
 * aborted (check org.apache.apex.malhar.lib.io.jms.SQSTestBase.generateCurrentQueueName(String))
 *
 */
public class SQSStringInputOperatorTest
{
  public static class TestMeta extends TestWatcher
  {
    String baseDir;
    JMSStringInputOperator operator;
    CollectorTestSink<Object> sink;
    Context.OperatorContext context;
    SQSTestBase testBase;

    @Override
    protected void starting(Description description)
    {
      final String methodName = description.getMethodName();
      final String className = description.getClassName();

      testBase = new SQSTestBase();
      if (testBase.validateTestCreds() == false) {
        return;
      }
      testBase.generateCurrentQueueName(methodName);
      try {
        testBase.beforTest();
      } catch (AssumptionViolatedException ave) {
        throw ave;
      } catch (Exception e) {
        throw new RuntimeException(e);
      }

      baseDir = "target/" + className + "/" + methodName;

      Attribute.AttributeMap attributeMap = new Attribute.AttributeMap.DefaultAttributeMap();
      attributeMap.put(Context.OperatorContext.SPIN_MILLIS, 500);
      attributeMap.put(Context.DAGContext.APPLICATION_PATH, baseDir);

      context = mockOperatorContext(1, attributeMap);
      operator = new JMSStringInputOperator();
      operator.setConnectionFactoryBuilder(new JMSBase.ConnectionFactoryBuilder()
      {

        @Override
        public ConnectionFactory buildConnectionFactory()
        {
          // Create the connection factory using the environment variable credential provider.
          // Connections this factory creates can talk to the queues in us-east-1 region.
          SQSConnectionFactory connectionFactory =
              SQSConnectionFactory.builder()
              .withRegion(Region.getRegion(Regions.US_EAST_1))
              .withAWSCredentialsProvider(new PropertiesFileCredentialsProvider(testBase.getDevCredsFilePath()))
              .build();
          return connectionFactory;
        }

        @Override
        public String toString()
        {
          return className + "/" + methodName + "/ConnectionFactoryBuilder";
        }

      });
      operator.setSubject(testBase.getCurrentQueueName());
      // for SQS ack mode should be "AUTO_ACKNOWLEDGE" and transacted = false
      operator.setAckMode("AUTO_ACKNOWLEDGE");
      operator.setTransacted(false);

      sink = new CollectorTestSink<>();
      operator.output.setSink(sink);
      operator.setup(context);
      operator.activate(context);
    }

    @Override
    protected void finished(Description description)
    {
      if (operator == null) {
        Assert.assertFalse(testBase.validateTestCreds());
        return;
      }
      operator.deactivate();
      operator.teardown();
      try {
        FileUtils.deleteDirectory(new File("target/" + description.getClassName()));
        testBase.afterTest();
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    }
  }

  @Rule
  public TestMeta testMeta = new TestMeta();


  /**
   * Basic string input test
   *
   * @throws Exception
   */
  @Test
  public void testStringMsgInput() throws Exception
  {
    testMeta.testBase.validateAssumption();
    testMeta.testBase.produceMsg("testStringMsgInput", 10, false);
    Thread.sleep(1000);
    testMeta.operator.emitTuples();
    Assert.assertEquals("num of messages", 10, testMeta.sink.collectedTuples.size());
  }


  @Test
  public void testRecoveryAndIdempotency() throws Exception
  {
    testMeta.testBase.validateAssumption();
    testMeta.testBase.produceUniqueMsgs("testRecoveryAndIdempotency", 25, false);
    Thread.sleep(3000);
    testMeta.operator.beginWindow(1);
    testMeta.operator.emitTuples();
    testMeta.operator.endWindow();

    Assert.assertEquals("num of messages in window 1 pre-failure", 25, testMeta.sink.collectedTuples.size());

    // for some reason AMZ SQS doesn't preserve the order producer->consumer or we might have a race
    //    condition on our producer side, so we can't be sure that get(4) will return "4:..."
    //    In any case we will only check message matching between pre-failure
    //    and post-failure cases for 4th and 17th message
    final String message4 = (String)testMeta.sink.collectedTuples.get(4);
    final String message17 = (String)testMeta.sink.collectedTuples.get(17);

    //failure and then re-deployment of operator
    testMeta.sink.collectedTuples.clear();
    testMeta.operator.setup(testMeta.context);
    testMeta.operator.activate(testMeta.context);

    Assert.assertEquals("largest recovery window", 1,
        testMeta.operator.getWindowDataManager().getLargestCompletedWindow());

    testMeta.operator.beginWindow(1);
    testMeta.operator.endWindow();
    Assert.assertEquals("num of messages in window 1", 25, testMeta.sink.collectedTuples.size());
    Assert.assertEquals(message4, testMeta.sink.collectedTuples.get(4));
    Assert.assertEquals(message17, testMeta.sink.collectedTuples.get(17));
    testMeta.sink.collectedTuples.clear();
  }


  /**
   * This test is different from the one in JMSStringInputOperatorTest because of the differences
   * in acknowledge mode. There is no Ack failure but rather failure in emit Tuple. But endWindow
   * eventually drains the holdingBuffer and emits all the outstanding tuples so we see 9 tuples
   * eventually. Because of the async nature of the operator (messages are delivered to the operator
   * as an async listener) we can't be sure how many messages are present in testMeta.sink.collectedTuples
   * before endWindow so our assertion is just testMeta.sink.collectedTuples.size() < 9
   *
   * @throws Exception
   */
  @Test
  public void testFailureAfterPersistenceAndBeforeRecovery() throws Exception
  {
    testMeta.testBase.validateAssumption();
    testMeta.sink = new CollectorTestSink<Object>()
    {
      @Override
      public void put(Object payload)
      {
        if (payload instanceof String && ((String)payload).startsWith("4:")) {
          throw new RuntimeException("fail 4th message");
        }
        synchronized (collectedTuples) {
          collectedTuples.add(payload);
          collectedTuples.notifyAll();
        }
      }
    };
    testMeta.operator.output.setSink(testMeta.sink);

    testMeta.testBase.produceUniqueMsgs("testFailureAfterPersistenceAndBeforeRecovery", 10, false);
    Thread.sleep(1000);
    testMeta.operator.beginWindow(1);
    try {
      testMeta.operator.emitTuples();
    } catch (Throwable t) {
      LOG.debug("emit exception");
    }
    Assert.assertTrue("num of messages before endWindow 1", testMeta.sink.collectedTuples.size() < 9);
    testMeta.operator.endWindow();
    Assert.assertEquals("num of messages after endWindow 1", 9, testMeta.sink.collectedTuples.size());

    testMeta.operator.setup(testMeta.context);
    testMeta.operator.activate(testMeta.context);

    Assert.assertEquals("window 1 should exist", 1,
        testMeta.operator.getWindowDataManager().getLargestCompletedWindow());
  }


  private static final transient Logger LOG = LoggerFactory.getLogger(SQSStringInputOperatorTest.class);
}
