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

import org.junit.After;
import org.junit.Assume;
import org.junit.Before;

import com.amazonaws.auth.PropertiesCredentials;
import com.amazonaws.auth.PropertiesFileCredentialsProvider;
import com.amazonaws.services.sqs.AmazonSQSClient;
import com.amazonaws.services.sqs.model.CreateQueueRequest;
import com.amazonaws.services.sqs.model.CreateQueueResult;
import com.amazonaws.services.sqs.model.ListQueuesResult;
import com.amazonaws.services.sqs.model.PurgeQueueRequest;

/**
 * Base class for SQS tests. <br/>
 * Various SQS (AWS) related helper functions
 */
public class SQSTestBase
{
  public static final String TEST_FOO = "TEST_FOO";

  public PropertiesCredentials testCreds;

  /**
   * creds to be used by the dev end eg. by the JMSInputOperator in SQS mode
   */
  private static String SQSDEV_CREDS_FILENAME = "/sqsdevCreds.properties";

  /**
   * creds to be used by the test end eg. by SQSInputOperatorTest
   */
  private static String SQSTEST_CREDS_FILENAME = "/sqstestCreds.properties";

  private AmazonSQSClient sqs;

  private String currentQueueName;
  private String currentQueueUrl;


  public SQSTestBase()
  {
    PropertiesFileCredentialsProvider file = new PropertiesFileCredentialsProvider(getTestCredsFilePath());
    testCreds = (PropertiesCredentials)file.getCredentials();
    sqs = new AmazonSQSClient(testCreds);
  }

  public String getTestCredsFilePath()
  {
    return getClass().getResource(SQSTEST_CREDS_FILENAME).getFile();
  }

  public String getDevCredsFilePath()
  {
    return getClass().getResource(SQSDEV_CREDS_FILENAME).getFile();
  }

  public String getCurrentQueueName()
  {
    return currentQueueName;
  }

  public void setCurrentQueueName(String currentQueueName)
  {
    this.currentQueueName = currentQueueName;
  }

  /**
   *  Each test creates its own uniquely named queue in SQS and then deletes it afterwards.
   *  We try to scrub any leftover queues from the previous runs just in case tests were
   * aborted
   *
   * @param currentQueueNamePrefix
   */
  public void generateCurrentQueueName(String currentQueueNamePrefix)
  {
    if (validateTestCreds()) {
      ListQueuesResult list = sqs.listQueues(currentQueueNamePrefix);
      for (String url : list.getQueueUrls()) {
        sqs.deleteQueue(url);
      }
    }
    this.currentQueueName = currentQueueNamePrefix + System.currentTimeMillis();
  }

  public void produceMsg(String[] msgs, boolean purgeFirst) throws Exception
  {
    CreateQueueResult res = sqs.createQueue(getCurrentQueueName());
    if (purgeFirst) {
      PurgeQueueRequest purgeReq = new PurgeQueueRequest(res.getQueueUrl());
      sqs.purgeQueue(purgeReq);
    }
    for (String text : msgs) {
      sqs.sendMessage(res.getQueueUrl(), text);
    }
  }

  /**
   *
   * @param text
   * @throws Exception
   */
  public void produceMsg(String text, boolean purgeFirst) throws Exception
  {
    produceMsg(new String[] {text}, purgeFirst);
  }

  /**
   * TODO: copy the logic of JMSTestBase.produceMsg
   *
   * @param text
   * @throws Exception
   */
  public void produceMsg(String text, int num, boolean purgeFirst) throws Exception
  {
    String[] array = new String[num];
    for (int i = 0; i < num; i++) {
      array[i] = text;
    }
    produceMsg(array, purgeFirst);
  }

  /**
   * Produce unique messages
   *
   * @param text
   * @throws Exception
   */
  public void produceUniqueMsgs(String text, int num, boolean purgeFirst) throws Exception
  {
    String[] array = new String[num];
    for (int i = 0; i < num; i++) {
      array[i] = "" + i + ":" + text;
    }
    produceMsg(array, purgeFirst);
  }

  public boolean validateTestCreds()
  {
    return testCreds.getAWSSecretKey() != null &&
        testCreds.getAWSSecretKey().trim().isEmpty() == false;
  }

  public void validateAssumption()
  {
    Assume.assumeTrue(validateTestCreds());
  }


  /**
   * create a queue we can use for testing
   *
   * @throws Exception
   */
  @Before
  public void beforTest() throws Exception
  {
    validateAssumption();
    // Create a queue
    CreateQueueRequest createQueueRequest = new CreateQueueRequest().withQueueName(getCurrentQueueName());
    currentQueueUrl = sqs.createQueue(createQueueRequest).getQueueUrl();
  }

  @After
  public void afterTest() throws Exception
  {
    sqs.deleteQueue(currentQueueUrl);
  }
}
