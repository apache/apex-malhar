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

import com.datatorrent.api.BaseOperator;
import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.api.Context.OperatorContext;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import javax.mail.*;
import javax.mail.internet.InternetAddress;
import javax.mail.internet.MimeMessage;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This operator outputs data to an smtp server.
 * <p></p>
 * @displayName Smtp Output
 * @category io
 * @tags stmp, output
 *
 * @since 0.3.2
 */
public class SmtpOutputOperator extends BaseOperator
{
  public enum RecipientType
  {
    TO, CC, BCC
  };

  private static final Logger LOG = LoggerFactory.getLogger(SmtpOutputOperator.class);
  protected String subject;
  protected String content;
  protected transient Session session;
  protected transient Message message;
  protected String from;
  protected Map<RecipientType, ArrayList<String>> recAddresses = new HashMap<RecipientType, ArrayList<String>>();
  protected transient Properties properties = System.getProperties();
  protected transient Authenticator auth;
  protected int smtpPort = 587;
  protected String smtpHost;
  protected String smtpUserName;
  protected String smtpPassword;
  protected String contentType = "text/plain";
  protected boolean useSsl = false;
  protected boolean setupCalled = false;
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
      }
      catch (MessagingException ex) {
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

  public void addRecipient(RecipientType type, String rec)
  {
    if (!recAddresses.containsKey(type)) {
      recAddresses.put(type, new ArrayList<String>());
    }
    recAddresses.get(type).add(rec);
    resetMessage();
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
      for (Map.Entry<RecipientType, ArrayList<String>> entry: recAddresses.entrySet()) {
        for (String addr: entry.getValue()) {
          Message.RecipientType mtype;
          switch (entry.getKey()) {
            case TO:
              mtype = Message.RecipientType.TO;
              break;
            case CC:
              mtype = Message.RecipientType.CC;
              break;
            case BCC:
            default:
              mtype = Message.RecipientType.BCC;
              break;
          }
          message.addRecipient(mtype, new InternetAddress(addr));
        }
      }
      message.setSubject(subject);
    }
    catch (MessagingException ex) {
      LOG.error(ex.toString());
    }

  }

}
