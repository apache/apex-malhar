/*
 * Copyright (c) 2015 DataTorrent, Inc. ALL Rights Reserved.
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
package com.datatorrent.lib.io.smtp;

import java.io.File;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

import javax.mail.Message;
import javax.mail.internet.InternetAddress;
import javax.mail.internet.MimeMessage;

import com.esotericsoftware.kryo.Kryo;
import com.google.common.collect.Maps;
import com.icegreen.greenmail.util.GreenMail;
import com.icegreen.greenmail.util.ServerSetup;
import com.icegreen.greenmail.util.ServerSetupTest;

import org.junit.*;
import org.junit.Assert;
import org.junit.Test;
import org.junit.rules.TestWatcher;
import org.junit.runner.Description;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;

import com.datatorrent.lib.helper.OperatorContextTestHelper;
import com.datatorrent.lib.io.IdempotentStorageManager;
import com.datatorrent.lib.util.TestUtils;

import com.datatorrent.api.*;
import java.util.logging.Level;

public class SmtpOutputOperatorTest
{

  String subject = "ALERT!";
  String content = "This is an SMTP operator test {}";
  String from = "jenkins@datatorrent.com";
  String to = "jenkins@datatorrent.com";
  String cc = "jenkinsCC@datatorrent.com";
  GreenMail greenMail = null;
  SmtpIdempotentOutputOperator node;

  Map<String, String> data;

  public class TestMeta extends TestWatcher
  {
    public String dir = null;
    Context.OperatorContext context;
    IdempotentStorageManager.FSIdempotentStorageManager manager = new IdempotentStorageManager.FSIdempotentStorageManager();
    @Override
    protected void starting(org.junit.runner.Description description)
    {
      String methodName = description.getMethodName();
      String className = description.getClassName();
      this.dir = "target/" + className + "/" + methodName;
      Attribute.AttributeMap attributes = new Attribute.AttributeMap.DefaultAttributeMap();
      attributes.put(DAG.DAGContext.APPLICATION_ID, "FileInputOperatorTest");
      context = new OperatorContextTestHelper.TestIdOperatorContext(1, attributes);
      node = new SmtpIdempotentOutputOperator();
      greenMail = new GreenMail(ServerSetupTest.ALL);
      greenMail.start();
      node.setFrom(from);
      node.setContent(content);
      node.setSmtpHost("127.0.0.1");
      node.setSmtpPort(ServerSetupTest.getPortOffset() + ServerSetup.SMTP.getPort());
      node.setSmtpUserName(from);
      node.setSmtpPassword("<password>");
      data = new HashMap<String, String>();
      data.put("alertkey", "alertvalue");

    manager.setRecoveryPath(testMeta.dir + "/recovery");
    manager.setup(testMeta.context);
    node.setIdempotentStorageManager(manager);

    }

    @Override
    protected void finished(Description description)
    {
      super.finished(description);
      FileUtils.deleteQuietly(new File(this.dir));
      greenMail.stop();
      try {
        Thread.sleep(1000);
      }
      catch (InterruptedException ex) {
        throw new RuntimeException(ex);
      }
    }

  }

  @Rule
  public TestMeta testMeta = new TestMeta();


  @Test
  public void testSmtpOutputNode() throws Exception
  {
    Map<String, String> recipients = Maps.newHashMap();
    recipients.put("to", to + "," + cc);
    recipients.put("cc", cc);
    node.setRecipients(recipients);
    node.setSubject("hello");
    node.setup(testMeta.context);
    node.beginWindow(0);
    String data = "First test message";
    node.input.process(data);
    Thread.sleep(5000);
    node.endWindow();
    node.teardown();
    Thread.sleep(5000);
    Assert.assertTrue(greenMail.waitForIncomingEmail(5000, 1));
    MimeMessage[] messages = greenMail.getReceivedMessages();
    Assert.assertEquals(3, messages.length);
    String receivedContent = messages[0].getContent().toString().trim();
    String expectedContent = content.replace("{}", data.toString()).trim();

    Assert.assertTrue(expectedContent.equals(receivedContent));
    Assert.assertEquals(from, ((InternetAddress)messages[0].getFrom()[0]).getAddress());
    Assert.assertEquals(to, messages[0].getRecipients(Message.RecipientType.TO)[0].toString());
    Assert.assertEquals(cc, messages[0].getRecipients(Message.RecipientType.TO)[1].toString());
    Assert.assertEquals(cc, messages[0].getRecipients(Message.RecipientType.CC)[0].toString());
  }

  @Test
  public void test() throws Exception
  {
    Map<String, String> recipients = Maps.newHashMap();
    recipients.put("to", to);
    node.setRecipients(recipients);
    node.setup(testMeta.context);
    node.beginWindow(0);
    String data = "First test message";
    node.input.process(data);
    node.endWindow();
    node.teardown();
    Thread.sleep(500);
    Assert.assertTrue(greenMail.waitForIncomingEmail(5000, 1));
    MimeMessage[] messages = greenMail.getReceivedMessages();
    Assert.assertEquals(1, messages.length);
    String receivedContent = messages[0].getContent().toString().trim();
    String expectedContent = content.replace("{}", data.toString()).trim();

    Assert.assertTrue(expectedContent.equals(receivedContent));
    Assert.assertEquals(from, ((InternetAddress)messages[0].getFrom()[0]).getAddress());
    Assert.assertEquals(to, messages[0].getAllRecipients()[0].toString());
  }

  @Test
  public void testProperties() throws Exception
  {
    Configuration conf = new Configuration(false);
    conf.set(StreamingApplication.DT_PREFIX + "operator.o1.prop.subject", subject);
    conf.set(StreamingApplication.DT_PREFIX + "operator.o1.prop.content", content);
    conf.set(StreamingApplication.DT_PREFIX + "operator.o1.prop.from", from);
    conf.set(StreamingApplication.DT_PREFIX + "operator.o1.prop.smtpHost", "127.0.0.1");
    conf.set(StreamingApplication.DT_PREFIX + "operator.o1.prop.smtpUserName", from);
    conf.set(StreamingApplication.DT_PREFIX + "operator.o1.prop.smtpPassword", "<password>");
    conf.set(StreamingApplication.DT_PREFIX + "operator.o1.prop.recipients.TO", to + "," + cc);
    conf.set(StreamingApplication.DT_PREFIX + "operator.o1.prop.recipients.CC", cc);

    final AtomicReference<SmtpIdempotentOutputOperator> o1 = new AtomicReference<SmtpIdempotentOutputOperator>();
    StreamingApplication app = new StreamingApplication()
    {
      @Override
      public void populateDAG(DAG dag, Configuration conf)
      {
        o1.set(dag.addOperator("o1", new SmtpIdempotentOutputOperator()));
      }

    };

    LocalMode lma = LocalMode.newInstance();
    lma.prepareDAG(app, conf);
    Assert.assertEquals("checking TO list", to + "," + cc, o1.get().getRecipients().get("TO"));
    Assert.assertEquals("checking CC list", cc, o1.get().getRecipients().get("CC"));
  }

  @Test
  public void testIdempotency() throws Exception
  {
    Map<String, String> recipients = Maps.newHashMap();

    recipients.put("to", to);
    node.setup(testMeta.context);
    node.setRecipients(recipients);
    node.setSubject("hello");
    for (long wid = 1; wid < 10; wid++) {
      node.beginWindow(wid);
      String input = wid + "test message";
      node.input.process(input);
      if(wid == 5 || wid == 6 || wid == 7){
      input = wid +"hello"+"test message";
       node.input.process(input);
      }
      node.endWindow();
      Thread.sleep(10000);
      if (wid == 7) {
        SmtpIdempotentOutputOperator newOp = TestUtils.clone(new Kryo(), node);
        node.teardown();
        newOp.setup(testMeta.context);
        newOp.beginWindow(5);
        String inputTest = 5 + "test message";
        newOp.input.process(inputTest);
        inputTest = 5 +"hello"+"test message";
        newOp.endWindow();
        Thread.sleep(5000);

        newOp.beginWindow(6);
        inputTest = 6 + "test message";
        newOp.input.process(inputTest);
        inputTest = 6 +"hello"+"test message";
        newOp.input.process(inputTest);
        newOp.endWindow();
        Thread.sleep(5000);

        newOp.beginWindow(7);
        inputTest = 7 + "test message";
        newOp.input.process(inputTest);
        inputTest = 7 +"hello"+"test message";
        newOp.input.process(inputTest);
        newOp.endWindow();
        Thread.sleep(5000);
        //normal processing
        newOp.beginWindow(8);
        inputTest = 8 + "test message";
        newOp.input.process(inputTest);
        newOp.endWindow();
        Thread.sleep(10000);
        newOp.teardown();
        break;
      }
    }
    Assert.assertTrue(greenMail.waitForIncomingEmail(5000, 1));

    MimeMessage[] messages = greenMail.getReceivedMessages();
    Assert.assertEquals(11, messages.length);
    for (int i = 0; i < 5; i++) {
      String receivedContent = messages[i].getContent().toString().trim();
      logger.debug("received content is {}", receivedContent);
      String expectedContent = content.replace("{}", i+1 + "test message").trim();
      Assert.assertTrue(expectedContent.equals(receivedContent));
    }

    String receivedContent = messages[5].getContent().toString().trim();
    logger.debug("received content is {}", receivedContent);
    String expectedContent = content.replace("{}", 5 + "hellotest message").trim();
    Assert.assertTrue(expectedContent.equals(receivedContent));
    receivedContent = messages[6].getContent().toString().trim();
    logger.debug("received content is {}", receivedContent);
    expectedContent = content.replace("{}", 6 + "test message").trim();
    Assert.assertTrue(expectedContent.equals(receivedContent));
    receivedContent = messages[7].getContent().toString().trim();
    logger.debug("received content is {}", receivedContent);
    expectedContent = content.replace("{}", 6 + "hellotest message").trim();
    Assert.assertTrue(expectedContent.equals(receivedContent));
    receivedContent = messages[8].getContent().toString().trim();
    logger.debug("received content is {}", receivedContent);
    expectedContent = content.replace("{}", 7 + "test message").trim();
    Assert.assertTrue(expectedContent.equals(receivedContent));
    receivedContent = messages[9].getContent().toString().trim();
    logger.debug("received content is {}", receivedContent);
    expectedContent = content.replace("{}", 7 + "hellotest message").trim();
    Assert.assertTrue(expectedContent.equals(receivedContent));
    receivedContent = messages[10].getContent().toString().trim();
    logger.debug("received content is {}", receivedContent);
    expectedContent = content.replace("{}", 8 + "test message").trim();
    Assert.assertTrue(expectedContent.equals(receivedContent));

    Assert.assertEquals(from, ((InternetAddress)messages[0].getFrom()[0]).getAddress());
    Assert.assertEquals(to, messages[0].getRecipients(Message.RecipientType.TO)[0].toString());
  }

  private static final Logger logger = LoggerFactory.getLogger(SmtpOutputOperatorTest.class);
}