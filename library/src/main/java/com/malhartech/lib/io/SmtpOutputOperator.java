/*
 *  Copyright (c) 2012 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.malhartech.lib.io;

import com.malhartech.annotation.ShipContainingJars;
import com.malhartech.api.BaseOperator;
import com.malhartech.api.Context.OperatorContext;
import com.malhartech.api.DefaultInputPort;
import com.sun.jersey.api.client.Client;
import com.sun.jersey.api.client.WebResource;
import java.net.URI;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.logging.Level;
import javax.mail.*;
import javax.mail.Message.RecipientType;
import javax.mail.internet.InternetAddress;
import javax.mail.internet.MimeMessage;
import javax.ws.rs.core.MediaType;
import org.apache.commons.lang3.StringUtils;
import org.codehaus.jettison.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class SmtpOutputOperator<T> extends BaseOperator
{
  private static final Logger LOG = LoggerFactory.getLogger(SmtpOutputOperator.class);
  protected String subject;
  protected String content;
  protected Session session;
  protected Message message;
  protected InternetAddress fromAddress;
  protected HashMap<Message.RecipientType, ArrayList<InternetAddress>> recAddresses;
  protected Properties properties = System.getProperties();
  protected Authenticator auth;
  protected int smtpPort = 587;
  protected String smtpHost;
  protected String smtpUserName;
  protected String smtpPassword;
  protected String contentType = "text/plain";
  public final transient DefaultInputPort<T> input = new DefaultInputPort<T>(this)
  {
    @Override
    public void process(T t)
    {
      try {
        String mailContent = content.replace("\\{\\}", t.toString());
        message.setContent(mailContent, contentType);
        Transport.send(message);
      }
      catch (MessagingException ex) {
        java.util.logging.Logger.getLogger(SmtpOutputOperator.class.getName()).log(Level.SEVERE, null, ex);
      }

    }

  };

  public void setContentType(String type)
  {
    contentType = type;
  }

  public void setSmtpHost(String host)
  {
    this.smtpHost = host;
  }

  public void setSmtpPort(int port)
  {
    this.smtpPort = port;
  }

  public void setSmtpUser(String user)
  {
    this.smtpUserName = user;
  }

  public void setSmtpPassword(String password)
  {
    this.smtpPassword = password;
  }

  public void setFrom(InternetAddress from)
  {
    fromAddress = from;
  }

  public void addRecipient(Message.RecipientType type, InternetAddress rec)
  {
    if (!recAddresses.containsKey(type)) {
      recAddresses.put(type, new ArrayList<InternetAddress>());
    }
    recAddresses.get(type).add(rec);
  }

  public void setSubject(String subject)
  {
    this.subject = subject;
  }

  public void setContent(String content, String mimeType)
  {
    this.content = content;
  }

  @Override
  public void setup(OperatorContext context)
  {
    super.setup(context);

    if (!StringUtils.isBlank(smtpPassword)) {
      properties.setProperty("mail.smtp.auth", "true");
      properties.setProperty("mail.smtp.starttls.enable", "true");

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
      message.setFrom(fromAddress);
      for (Map.Entry<RecipientType, ArrayList<InternetAddress>> entry: recAddresses.entrySet()) {
        for (InternetAddress addr: entry.getValue()) {
          message.addRecipient(entry.getKey(), addr);
        }
      }
      message.setSubject(subject);
    }
    catch (MessagingException ex) {
      java.util.logging.Logger.getLogger(SmtpOutputOperator.class.getName()).log(Level.SEVERE, null, ex);
    }

  }

}
