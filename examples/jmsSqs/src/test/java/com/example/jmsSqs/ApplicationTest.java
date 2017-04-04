/**
 * Put your copyright and license info here.
 */
package com.example.jmsSqs;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.util.HashSet;

import javax.validation.ConstraintViolationException;

import org.junit.Assert;

import org.apache.hadoop.conf.Configuration;
import org.elasticmq.rest.sqs.SQSRestServer;
import org.elasticmq.rest.sqs.SQSRestServerBuilder;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.sqs.AmazonSQSClient;
import com.amazonaws.services.sqs.model.CreateQueueResult;
import com.amazonaws.services.sqs.model.PurgeQueueRequest;
import com.datatorrent.api.LocalMode;

/**
 * Test the DAG declaration in local mode.
 */
public class ApplicationTest {

  private static final Logger LOG = LoggerFactory.getLogger(ApplicationTest.class);

  private static final String FILE_NAME = "test";
  private static final String FILE_DIR  = "target/jmsSQS";
  private static final String FILE_PATH = FILE_DIR + "/" + FILE_NAME + ".0";     // first part

  private static final String QUEUE_NAME_PREFIX = "jms4Sqs";

  private String currentQueueName;

  private String currentQueueUrl;

  private AmazonSQSClient sqs;

  private SQSRestServer mockServer;

  // test messages
  private static String[] lines =
  {
    "1st line",
    "2nd line",
    "3rd line",
    "4th line",
    "5th line",
  };

  @Test
  public void testApplication() throws Exception {
    try {
      // delete output file if it exists
      File file = new File(FILE_PATH);
      file.delete();

      // Each run creates its own uniquely named queue in SQS and then deletes it afterwards.
      //  because SQS doesn't allow a deleted queue to be reused within 60 seconds
      currentQueueName = QUEUE_NAME_PREFIX + System.currentTimeMillis();

      createSQSClient();

      // write messages to the SQS Queue
      writeToQueue();

      // run app asynchronously; terminate after results are checked
      LocalMode.Controller lc = asyncRun();

      // check for presence of output file
      chkOutput();

      // compare output lines to input
      compare();

      lc.shutdown();
    } catch (ConstraintViolationException e) {
      Assert.fail("constraint violations: " + e.getConstraintViolations());
    }
  }

  private void createSQSClient()
  {
    createElasticMQMockServer();
    // just use x and y as access and secret keys respectively: not checked by the mock server
    sqs = new AmazonSQSClient(new BasicAWSCredentials("x", "y"));
    // have it talk to the embedded server
    sqs.setEndpoint("http://localhost:9324");
  }

  private void createElasticMQMockServer()
  {
    // By default the urls will use http://localhost:9324 as the base URL
    mockServer = SQSRestServerBuilder.start();
  }

  private void writeMsg(String[] msgs)
  {
    CreateQueueResult res = sqs.createQueue(currentQueueName);

    currentQueueUrl = res.getQueueUrl();

    // we should purge the queue first
    PurgeQueueRequest purgeReq = new PurgeQueueRequest(currentQueueUrl);
    sqs.purgeQueue(purgeReq);
    for (String text : msgs) {
      sqs.sendMessage(currentQueueUrl, text);
    }
  }

  private void writeToQueue() {
    writeMsg(lines);
    LOG.debug("Sent messages to topic {}", QUEUE_NAME_PREFIX);
  }

  private Configuration getConfig() {
    Configuration conf = new Configuration(false);

    // read config values from the properties.xml file
    conf.addResource(this.getClass().getResourceAsStream("/META-INF/properties.xml"));

    // one can also set or override values in code as below
    String pre = "dt.operator.fileOut.prop.";
    conf.set(pre + "filePath", FILE_DIR);

    pre = "dt.operator.sqsIn.prop.";
    // set the subject here since it is dynamically generated
    conf.set(pre + "subject", currentQueueName);
    // set the endpoint to point to our mock server
    conf.set(pre + "aws.endpoint", "http://localhost:9324");

    return conf;
  }

  private static void chkOutput() throws Exception {
    File file = new File(FILE_PATH);
    final int MAX = 60;
    for (int i = 0; i < MAX && (! file.exists()); ++i ) {
      LOG.debug("Sleeping, i = {}", i);
      Thread.sleep(1000);
    }
    if (! file.exists()) {
      String msg = String.format("Error: %s not found after %d seconds%n", FILE_PATH, MAX);
      throw new RuntimeException(msg);
    }
  }

  private void compare() throws Exception {
    // read output file
    File file = new File(FILE_PATH);
    BufferedReader br = new BufferedReader(new FileReader(file));

    HashSet<String> set = new HashSet<String>();
    String line;
    while (null != (line = br.readLine())) {
      set.add(line);
    }
    br.close();

    // now delete the file, we don't need it anymore
    Assert.assertTrue("Deleting "+file, file.delete());

    // delete the current queue, since the Queue's job is done
    sqs.deleteQueue(currentQueueUrl);

    // compare
    Assert.assertEquals("number of lines", lines.length, set.size());
    for (int i = 0; i < lines.length; ++i) {
      Assert.assertTrue("set contains "+lines[i], set.remove(lines[i]));
    }
  }

  private LocalMode.Controller asyncRun() throws Exception {
    Configuration conf = getConfig();
    LocalMode lma = LocalMode.newInstance();
    lma.prepareDAG(new SqsApplication(), conf);
    LocalMode.Controller lc = lma.getController();
    lc.runAsync();
    return lc;
  }


}
