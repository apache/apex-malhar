/**
 * Copyright (c) 2012-2012 Malhar, Inc.
 * All rights reserved.
 */
package com.malhartech.lib.io;

import java.util.HashMap;
import java.util.Map;


public class SmtpOutputOperatorTest {

  //@Test
  public void testSmtpOutputNode() throws Exception {

    SmtpOutputOperator<Object> node = new SmtpOutputOperator<Object>();
    node.setFrom("jenkins@malhar-inc.com");
    node.addRecipient(SmtpOutputOperator.RecipientType.TO, "jenkins@malhar-inc.com");
    node.setContent("This is an SMTP operator test: {}");
    node.setSmtpHost("secure.emailsrvr.com");
    node.setSmtpPort(465);
    node.setSmtpUserName("jenkins@malhar-inc.com");
    node.setSmtpPassword("Testing1");
    node.setUseSsl(true);
    node.setSubject("ALERT!");

    node.setup(null);

    Map<String, String> data = new HashMap<String, String>();
    data.put("alert key", "alert value");
    node.beginWindow(1000);
    node.input.process(data);
    node.endWindow();
    node.teardown();

  }

}
