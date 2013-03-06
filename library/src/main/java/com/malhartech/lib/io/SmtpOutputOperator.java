/*
 *  Copyright (c) 2012 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.malhartech.lib.io;

import com.malhartech.annotation.ShipContainingJars;
import com.malhartech.api.BaseOperator;
import com.malhartech.api.Context.OperatorContext;
import com.malhartech.api.DefaultInputPort;
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

@ShipContainingJars(classes = {javax.mail.Session.class})
public class SmtpOutputOperator<T> extends BaseOperator
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
  public final transient DefaultInputPort<T> input = new DefaultInputPort<T>(this)
  {
    @Override
    public void process(T t)
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
  }

  public String getContent()
  {
    return content;
  }

  public void setContent(String content)
  {
    this.content = content;
  }

  public String getFrom()
  {
    return from;
  }

  public void setFrom(String from)
  {
    this.from = from;
  }

  public int getSmtpPort()
  {
    return smtpPort;
  }

  public void setSmtpPort(int smtpPort)
  {
    this.smtpPort = smtpPort;
  }

  public String getSmtpHost()
  {
    return smtpHost;
  }

  public void setSmtpHost(String smtpHost)
  {
    this.smtpHost = smtpHost;
  }

  public String getSmtpUserName()
  {
    return smtpUserName;
  }

  public void setSmtpUserName(String smtpUserName)
  {
    this.smtpUserName = smtpUserName;
  }

  public String getSmtpPassword()
  {
    return smtpPassword;
  }

  public void setSmtpPassword(String smtpPassword)
  {
    this.smtpPassword = smtpPassword;
  }

  public String getContentType()
  {
    return contentType;
  }

  public void setContentType(String contentType)
  {
    this.contentType = contentType;
  }

  public boolean isUseSsl()
  {
    return useSsl;
  }

  public void setUseSsl(boolean useSsl)
  {
    this.useSsl = useSsl;
  }

  public void addRecipient(RecipientType type, String rec)
  {
    if (!recAddresses.containsKey(type)) {
      recAddresses.put(type, new ArrayList<String>());
    }
    recAddresses.get(type).add(rec);
  }

  @Override
  public void setup(OperatorContext context)
  {
    super.setup(context);

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
