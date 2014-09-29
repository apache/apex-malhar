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
package com.datatorrent.lib.io;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.icegreen.greenmail.util.GreenMail;
import com.icegreen.greenmail.util.ServerSetupTest;
import com.icegreen.greenmail.util.ServerSetup;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.mail.Message;
import javax.mail.internet.InternetAddress;
import javax.mail.internet.MimeMessage;

import org.apache.hadoop.conf.Configuration;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.datatorrent.api.StreamingApplication;

import com.datatorrent.stram.plan.logical.LogicalPlan;
import com.datatorrent.stram.plan.logical.LogicalPlanConfiguration;

public class SmtpOutputOperatorTest
{

  String subject = "ALERT!";
  String content = "This is an SMTP operator test {}";
  String from = "jenkins@datatorrent.com";
  String to = "jenkins@datatorrent.com";
  String cc = "jenkinsCC@datatorrent.com";
  GreenMail greenMail = null;
  SmtpOutputOperator node;

  @Before
  public void setup() throws Exception
  {
    node = new SmtpOutputOperator();
    greenMail = new GreenMail(ServerSetupTest.ALL);
    greenMail.start();
    node.setFrom(from);
    node.setContent(content);
    node.setSmtpHost("127.0.0.1");
    node.setSmtpPort(ServerSetupTest.getPortOffset() + ServerSetup.SMTP.getPort());
    node.setSmtpUserName(from);
    node.setSmtpPassword("<password>");
    //node.setUseSsl(true);
    node.setSubject(subject);

  }

  @After
  public void teardown() throws Exception
  {
    node.teardown();
    greenMail.stop();
    Thread.sleep(1000);
  }

  @Test
  public void testSmtpOutputNode() throws Exception
  {
    Map<String, String> recipients = Maps.newHashMap();
    recipients.put("to", to + "," + cc);
    recipients.put("cc", cc);
    node.setRecipients(recipients);
    node.setup(null);
    Map<String, String> data = new HashMap<String, String>();
    data.put("alertkey", "alertvalue");
    node.beginWindow(1000);
    node.input.process(data);
    node.endWindow();
    Assert.assertTrue(greenMail.waitForIncomingEmail(5000, 1));
    MimeMessage[] messages = greenMail.getReceivedMessages();
    Assert.assertEquals(3, messages.length);
    String receivedContent = messages[0].getContent().toString().trim();
    String expectedContent = content.replace("{}", data.toString()).trim();

    Assert.assertTrue(expectedContent.equals(receivedContent));
    Assert.assertEquals(from, ((InternetAddress) messages[0].getFrom()[0]).getAddress());
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
    node.setup(null);
    Map<String, String> data = new HashMap<String, String>();
    data.put("alertkey", "alertvalue");
    node.beginWindow(1000);
    node.input.process(data);
    node.endWindow();
    Assert.assertTrue(greenMail.waitForIncomingEmail(5000, 1));
    MimeMessage[] messages = greenMail.getReceivedMessages();
    Assert.assertEquals(1, messages.length);
    String receivedContent = messages[0].getContent().toString().trim();
    String expectedContent = content.replace("{}", data.toString()).trim();

    Assert.assertTrue(expectedContent.equals(receivedContent));
    Assert.assertEquals(from, ((InternetAddress) messages[0].getFrom()[0]).getAddress());
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

    LogicalPlan dag = new LogicalPlan();
    SmtpOutputOperator o1 = dag.addOperator("o1", new SmtpOutputOperator());
    LogicalPlanConfiguration pb = new LogicalPlanConfiguration();
    pb.addFromConfiguration(conf);

    pb.setOperatorProperties(dag, "testSetOperatorProperties");
    Assert.assertEquals("checking TO list", to + "," + cc, o1.getRecipients().get("TO"));
    Assert.assertEquals("checking CC list", cc, o1.getRecipients().get("CC"));

  }
}
