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

import com.datatorrent.api.BaseOperator;
import com.datatorrent.api.Context;
import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.api.Context.OperatorContext;
import com.datatorrent.api.Operator;
import com.datatorrent.common.util.DTThrowable;
import com.datatorrent.lib.io.IdempotentStorageManager;

import java.util.*;

import javax.mail.*;
import javax.mail.internet.InternetAddress;
import javax.mail.internet.MimeMessage;
import javax.validation.constraints.AssertTrue;
import javax.validation.constraints.NotNull;

import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Maps;
import java.io.IOException;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicReference;

/**
 * This operator outputs data to an smtp server.
 * <p>
 * </p>
 * @displayName Smtp Output
 * @category Output
 * @tags stmp, output operator
 *
 * @since 0.3.2
 */
public class SmtpIdempotentOutputOperator extends BaseOperator implements Operator.CheckpointListener, Operator.IdleTimeHandler
{
  public enum RecipientType
  {
    TO, CC, BCC
  }

  @NotNull
  private String subject;
  @NotNull
  private String content;
  @NotNull
  private String from;

  private Map<String, String> recipients = Maps.newHashMap();

  private int smtpPort = 689;
  private transient long sleepTimeMillis;
  @NotNull
  private String smtpHost;
  private String smtpUserName;
  private String smtpPassword;
  private String contentType = "text/plain";
  private boolean useSsl = false;
  private boolean setupCalled = false;
  private transient Queue<String> messagesSent;
  private transient SMTPSenderThread smtpSenderThread;
  protected transient Properties properties = System.getProperties();
  protected transient Authenticator auth;
  protected transient Session session;
  protected transient Message message;
  private final transient AtomicReference<Throwable> throwable;
  protected IdempotentStorageManager idempotentStorageManager = new IdempotentStorageManager.FSIdempotentStorageManager();
  protected Queue<String> waiting;
  protected int countMessageSent;
  private transient int countMessagesToBeSkipped;

  private transient long currentWindowId;
  private transient int operatorId;
  private transient long largestRecoveryWindow;
  private transient int countOfTuples;
  private transient int numWaiting;
  private transient final Object sync;

  public SmtpIdempotentOutputOperator()
  {
    throwable = new AtomicReference<Throwable>();
    waiting = new LinkedList<String>();
    sync = new Object();
  }

  @Override
  public void setup(OperatorContext context)
  {
    setupCalled = true;
    operatorId = context.getId();
    idempotentStorageManager.setup(context);
    sleepTimeMillis = context.getValue(Context.OperatorContext.SPIN_MILLIS);
    smtpSenderThread = new SMTPSenderThread();
    messagesSent = new LinkedList<String>();
    reset();
  }

  @Override
  public void beginWindow(long windowId)
  {
    countOfTuples = 0;
    largestRecoveryWindow = idempotentStorageManager.getLargestRecoveryWindow();
    currentWindowId = windowId;
    if (windowId <= largestRecoveryWindow) {
      numWaiting = waiting.size();
      countMessagesToBeSkipped = restore(windowId);
      if (countMessagesToBeSkipped > numWaiting) {
        // These messages were already sent in this window, so remove them from waiting.numWaiting could be size of waiting from previous window.
        for (int i = 0; i < numWaiting; i++) {
          waiting.poll();
        }
        // This diff could be number of tuples coming in this window, check in processTuple.
        countMessagesToBeSkipped = countMessagesToBeSkipped - numWaiting;

      }
      else if (countMessagesToBeSkipped <= numWaiting) //It is possible that rest of waiting messages were sent in next window.
      {
        for (int i = 0; i < countMessagesToBeSkipped; i++) {
          waiting.poll();
        }
        countMessagesToBeSkipped = 0;
      }
    }

  }

  protected void completed(String sentMessage)
  {
    messagesSent.add(sentMessage);
  }

  @Override
  public void teardown()
  {
    idempotentStorageManager.teardown();

    try {
      smtpSenderThread.stopService();
    }
    catch (IOException ex) {
      throw new RuntimeException(ex);
    }

    super.teardown();

  }

  @Override
  public void committed(long windowId)
  {
    try {
      idempotentStorageManager.deleteUpTo(operatorId, windowId);
    }
    catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * This is the port which receives the tuples that will be output to an smtp server.
   */
  public final transient DefaultInputPort<Object> input = new DefaultInputPort<Object>()
  {
    @Override
    public void process(Object t)
    {
      String mailContent = content.replace("{}", t.toString());
      // Recovery case
      if (currentWindowId <= largestRecoveryWindow) {
        countOfTuples++;
        /*There could be more duplicate messages coming in this window, we are not sure of state in waiting queue yet,
         hence add these to waiting queue. If they are already sent, then they will be removed in next or further
         windows according to logic implemented in beginwindow.*/
        if (countOfTuples > countMessagesToBeSkipped) {
          waiting.add(mailContent);
        }
        else {
          return;
        }
      }
      else {
        waiting.add(mailContent);
        smtpSenderThread.messageRcvdQueue.add(mailContent);
      }
    }

  };

  @Override
  public void handleIdleTime()
  {
    if (messagesSent.isEmpty() && throwable.get() == null) {
      try {
        Thread.sleep(sleepTimeMillis);
      }
      catch (InterruptedException ex) {
        throw new RuntimeException(ex);
      }
    }
    else if (throwable.get() != null) {
      DTThrowable.rethrow(throwable.get());
    }
    else {
      /**
       * Remove all the messages from waiting which have been sent.
       */
      if (!waiting.isEmpty()) {
        while (!messagesSent.isEmpty()) {
          waiting.remove(messagesSent.poll());
        }
      }
    }
  }

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
      for (Map.Entry<String, String> entry: recipients.entrySet()) {
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
        for (String address: addresses) {
          message.addRecipient(recipientType, new InternetAddress(address));
        }
      }
      message.setSubject(subject);
      LOG.debug("all recipients {}", Arrays.toString(message.getAllRecipients()));
    }
    catch (MessagingException ex) {
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
    for (Map.Entry<String, String> entry: recipients.entrySet()) {
      if (entry.getKey().toUpperCase().equalsIgnoreCase(RecipientType.TO.toString())) {
        if (entry.getValue() != null && entry.getValue().length() > 0) {
          return true;
        }
        return false;
      }
    }
    return false;
  }

  @Override
  public void endWindow()
  {
    if (currentWindowId == largestRecoveryWindow) {
      if (!waiting.isEmpty()) {
        smtpSenderThread.messageRcvdQueue.addAll(waiting);
      }
    }
    else if (currentWindowId > largestRecoveryWindow) {
      try {
        synchronized (sync) {
          idempotentStorageManager.save(countMessageSent, operatorId, currentWindowId);
          LOG.debug("idempotent manager saves count {}", countMessageSent);
          countMessageSent = 0;
        }
      }
      catch (IOException e) {
        throw new RuntimeException("saving recovery", e);
      }

    }
    if (!waiting.isEmpty()) {
      while (!messagesSent.isEmpty()) {
        waiting.remove(messagesSent.poll());
      }
    }
  }

  protected int restore(long windowId)
  {
    try {
      Map<Integer, Object> recoveryDataPerOperator = idempotentStorageManager.load(windowId);
      for (Object recovery: recoveryDataPerOperator.values()) {
        countMessagesToBeSkipped = (Integer)recovery;
      }
    }
    catch (IOException ex) {
      throw new RuntimeException("replay", ex);
    }
    return countMessagesToBeSkipped;
  }

  @Override
  public void checkpointed(long windowId)
  {
  }

  /**
   * Sets the idempotent storage manager on the operator.
   *
   * @param idempotentStorageManager an {@link IdempotentStorageManager}
   */
  public void setIdempotentStorageManager(IdempotentStorageManager idempotentStorageManager)
  {
    this.idempotentStorageManager = idempotentStorageManager;
  }

  /**
   * Returns the idempotent storage manager which is being used by the operator.
   *
   * @return the idempotent storage manager.
   */
  public IdempotentStorageManager getIdempotentStorageManager()
  {
    return idempotentStorageManager;
  }

  private class SMTPSenderThread implements Runnable
  {
    private transient final BlockingQueue<String> messageRcvdQueue;
    private transient volatile boolean running;

    SMTPSenderThread()
    {
      messageRcvdQueue = new LinkedBlockingQueue<String>();
      Thread messageServiceThread = new Thread(this, "SMTPSenderThread");
      messageServiceThread.start();
    }

    private void stopService() throws IOException
    {
      running = false;
    }

    @Override
    public void run()
    {
      running = true;
      while (running) {
        String mailContent = messageRcvdQueue.poll();
        if (mailContent != null) {
          try {
            message.setContent(mailContent, contentType);
            synchronized (sync) {
              Transport.send(message);
            }
          }
          catch (MessagingException ex) {
            running = false;
            throwable.set(ex);
          }
          synchronized (sync) {
            countMessageSent++;
          }

          LOG.debug("message is {}", message);
          completed(mailContent);

        }
      }

    }

  }

  private static final Logger LOG = LoggerFactory.getLogger(SmtpIdempotentOutputOperator.class);

}
