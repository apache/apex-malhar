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
package org.apache.apex.malhar.lib.io;

import java.util.Arrays;
import java.util.Map;
import java.util.Properties;

import javax.mail.Authenticator;
import javax.mail.Message;
import javax.mail.MessagingException;
import javax.mail.PasswordAuthentication;
import javax.mail.Session;
import javax.mail.Transport;
import javax.mail.internet.InternetAddress;
import javax.mail.internet.MimeMessage;
import javax.validation.constraints.AssertTrue;
import javax.validation.constraints.NotNull;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.commons.lang.StringUtils;

import com.google.common.collect.Maps;

import com.datatorrent.api.Context.OperatorContext;
import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.common.util.BaseOperator;

/**
 * This operator outputs data to an smtp server.
 * <p></p>
 * @displayName Smtp Output
 * @category Output
 * @tags stmp, output operator
 *
 * @since 0.3.2
 */
@org.apache.hadoop.classification.InterfaceStability.Evolving
public class SmtpOutputOperator extends BaseOperator
{
  public enum RecipientType
  {
    TO, CC, BCC
  }

  private static final Logger LOG = LoggerFactory.getLogger(SmtpOutputOperator.class);
  @NotNull
  private String subject;
  @NotNull
  private String content;
  @NotNull
  private String from;

  private Map<String, String> recipients = Maps.newHashMap();

  private int smtpPort = 587;
  @NotNull
  private String smtpHost;
  private String smtpUserName;
  private String smtpPassword;
  private String contentType = "text/plain";
  private boolean useSsl = false;
  private boolean setupCalled = false;

  protected transient Properties properties = System.getProperties();
  protected transient Authenticator auth;
  protected transient Session session;
  protected transient Message message;

  /**
   * This is the port which receives the tuples that will be output to an smtp server.
   */
  public final transient DefaultInputPort<Object> input = new DefaultInputPort<Object>()
  {
    @Override
    public void process(Object t)
    {
      try {
        String mailContent = content.replace("{}", t.toString());
        message.setContent(mailContent, contentType);
        LOG.info("Sending email for tuple {}", t.toString());
        Transport.send(message);
      } catch (MessagingException ex) {
        LOG.error("Something wrong with sending email.", ex);
      }
    }
  };

  public String getSubject()
  {
    return subject;
  }

  public void setSubject(String subject)
  {
    this.subject = subject;
    resetMessage();
  }

  public String getContent()
  {
    return content;
  }

  public void setContent(String content)
  {
    this.content = content;
    resetMessage();
  }

  public String getFrom()
  {
    return from;
  }

  public void setFrom(String from)
  {
    this.from = from;
    resetMessage();
  }

  public int getSmtpPort()
  {
    return smtpPort;
  }

  public void setSmtpPort(int smtpPort)
  {
    this.smtpPort = smtpPort;
    reset();
  }

  public String getSmtpHost()
  {
    return smtpHost;
  }

  public void setSmtpHost(String smtpHost)
  {
    this.smtpHost = smtpHost;
    reset();
  }

  public String getSmtpUserName()
  {
    return smtpUserName;
  }

  public void setSmtpUserName(String smtpUserName)
  {
    this.smtpUserName = smtpUserName;
    reset();
  }

  public String getSmtpPassword()
  {
    return smtpPassword;
  }

  public void setSmtpPassword(String smtpPassword)
  {
    this.smtpPassword = smtpPassword;
    reset();
  }

  public String getContentType()
  {
    return contentType;
  }

  public void setContentType(String contentType)
  {
    this.contentType = contentType;
    resetMessage();
  }

  public boolean isUseSsl()
  {
    return useSsl;
  }

  public void setUseSsl(boolean useSsl)
  {
    this.useSsl = useSsl;
    reset();
  }

  @Override
  public void setup(OperatorContext context)
  {
    setupCalled = true;
    reset();
  }

  private void reset()
  {
    if (!setupCalled) {
      return;
    }
    if (!StringUtils.isBlank(smtpPassword)) {
      properties.setProperty("mail.smtp.auth", "true");
      properties.setProperty("mail.smtp.starttls.enable", "true");
      if (useSsl) {
        properties.setProperty("mail.smtp.socketFactory.port", String.valueOf(smtpPort));
        properties.setProperty("mail.smtp.socketFactory.class", "javax.net.ssl.SSLSocketFactory");
        properties.setProperty("mail.smtp.socketFactory.fallback", "false");
      }

      auth = new Authenticator()
      {
        @Override
        protected PasswordAuthentication getPasswordAuthentication()
        {
          return new PasswordAuthentication(smtpUserName, smtpPassword);
        }

      };
    }

    properties.setProperty("mail.smtp.host", smtpHost);
    properties.setProperty("mail.smtp.port", String.valueOf(smtpPort));
    session = Session.getInstance(properties, auth);
    resetMessage();
  }

  private void resetMessage()
  {
    if (!setupCalled) {
      return;
    }
    try {
      message = new MimeMessage(session);
      message.setFrom(new InternetAddress(from));
      for (Map.Entry<String, String> entry : recipients.entrySet()) {
        RecipientType type = RecipientType.valueOf(entry.getKey().toUpperCase());
        Message.RecipientType recipientType;
        switch (type) {
          case TO:
            recipientType = Message.RecipientType.TO;
            break;
          case CC:
            recipientType = Message.RecipientType.CC;
            break;
          case BCC:
          default:
            recipientType = Message.RecipientType.BCC;
            break;
        }
        String[] addresses = entry.getValue().split(",");
        for (String address : addresses) {
          message.addRecipient(recipientType, new InternetAddress(address));
        }
      }
      message.setSubject(subject);
      LOG.debug("all recipients {}", Arrays.toString(message.getAllRecipients()));
    } catch (MessagingException ex) {
      throw new RuntimeException(ex);
    }
  }

  public Map<String, String> getRecipients()
  {
    return recipients;
  }

  /**
   * @param recipients : map from recipient type to coma separated list of addresses for e.g. to->abc@xyz.com,def@xyz.com
   */
  public void setRecipients(Map<String, String> recipients)
  {
    this.recipients = recipients;
    resetMessage();
  }

  @AssertTrue(message = "Please verify the recipients set")
  private boolean isValid()
  {
    if (recipients.isEmpty()) {
      return false;
    }
    for (Map.Entry<String, String> entry : recipients.entrySet()) {
      if (entry.getKey().toUpperCase().equalsIgnoreCase(RecipientType.TO.toString())) {
        if (entry.getValue() != null && entry.getValue().length() > 0) {
          return true;
        }
        return false;
      }
    }
    return false;
  }
}
