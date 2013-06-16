/**
 * Copyright (c) 2012-2012 Malhar, Inc.
 * All rights reserved.
 */
package com.malhartech.lib.io;

import com.icegreen.greenmail.util.GreenMail;
import com.icegreen.greenmail.util.ServerSetupTest;
import com.icegreen.greenmail.util.ServerSetup;
import java.util.HashMap;
import java.util.Map;
import javax.mail.internet.InternetAddress;
import javax.mail.internet.MimeMessage;
import org.junit.Assert;
import org.junit.Test;

public class SmtpOutputOperatorTest
{
  @Test
  public void testSmtpOutputNode() throws Exception
  {

    String subject = "ALERT!";
    String content = "This is an SMTP operator test {}";
    String from = "jenkins@malhar-inc.com";
    String to = "davidyan@malhar-inc.com";

    GreenMail greenMail = new GreenMail(ServerSetupTest.ALL);
    greenMail.start();

    SmtpOutputOperator node = new SmtpOutputOperator();
    node.setFrom(from);
    node.addRecipient(SmtpOutputOperator.RecipientType.TO, to);
    node.setContent(content);
    node.setSmtpHost("127.0.0.1");
    node.setSmtpPort(ServerSetupTest.getPortOffset() + ServerSetup.SMTP.getPort());
    node.setSmtpUserName(from);
    node.setSmtpPassword("<password>");
    //node.setUseSsl(true);
    node.setSubject(subject);

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
    Assert.assertEquals(from, ((InternetAddress)messages[0].getFrom()[0]).getAddress());
    Assert.assertEquals(to, messages[0].getAllRecipients()[0].toString());
    node.teardown();
    greenMail.stop();
  }

}
