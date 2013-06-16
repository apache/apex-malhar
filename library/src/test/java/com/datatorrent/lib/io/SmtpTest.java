package com.malhartech.lib.io;

import java.util.Properties;

import javax.mail.Message;
import javax.mail.MessagingException;
import javax.mail.PasswordAuthentication;
import javax.mail.Session;
import javax.mail.Transport;
import javax.mail.internet.InternetAddress;
import javax.mail.internet.MimeMessage;

import org.apache.commons.lang.StringUtils;
import org.junit.Test;


public class SmtpTest {

  @org.junit.Ignore
  @Test
  public void testSendEmail() throws Exception {

    final String smtpUsername = "youraccount@gmail.com";
    final String smtpPassword = "<application_specific_gmail_password"; // application specific gmail password
    String smtpHost = "smtp.gmail.com";
    Integer smtpPort = new Integer(587);

    // Recipient's email ID needs to be mentioned.
    String to = "all@malhar-inc.com";
    String from = "someone@malhar-inc.com";

    // Get system properties
    Properties properties = System.getProperties();
    javax.mail.Authenticator auth = null;

    if (!StringUtils.isBlank(smtpPassword)) {
      properties.setProperty("mail.smtp.auth", "true");
      properties.setProperty("mail.smtp.starttls.enable", "true");

      auth = new javax.mail.Authenticator() {
        @Override
        protected PasswordAuthentication getPasswordAuthentication() {
          return new PasswordAuthentication(smtpUsername, smtpPassword);
        }
      };
    }
    properties.setProperty("mail.smtp.host", smtpHost);
    if (smtpPort != null) {
      properties.setProperty("mail.smtp.port", Integer.toString(smtpPort));
    }

    // Get the default Session object.
    Session session = Session.getInstance(properties, auth);
    try{
       // Create a default MimeMessage object.
       Message message = new MimeMessage(session);

       // Set From: header field of the header.
       message.setFrom(new InternetAddress(from));

       // Set To: header field of the header.
       message.addRecipient(Message.RecipientType.TO,
                                new InternetAddress(to));

       message.setSubject("Testing Email for Alerts!");
       message.setContent("Testing Message Body\n\nEOM", "text/html");

       // Send message
       Transport.send(message);
       System.out.println("Sent message successfully....");
    }catch (MessagingException mex) {
       mex.printStackTrace();
    }

  }

}
