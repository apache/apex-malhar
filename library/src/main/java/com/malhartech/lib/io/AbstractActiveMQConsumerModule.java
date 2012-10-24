/*
 *  Copyright (c) 2012 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.malhartech.lib.io;

import com.malhartech.api.BaseOperator;
import com.malhartech.api.OperatorConfiguration;
import com.malhartech.api.SyncInputOperator;
import java.util.logging.Level;
import javax.jms.ExceptionListener;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author Locknath Shil <locknath@malhar-inc.com>
 */
public abstract class AbstractActiveMQConsumerModule extends BaseOperator implements SyncInputOperator, Runnable, MessageListener, ExceptionListener
{
  private static final Logger logger = LoggerFactory.getLogger(AbstractActiveMQConsumerModule.class);
  private long maxMessages;
  public ActiveMQHelper activeMQHelper = new ActiveMQHelper(false);

  /**
   * Any concrete class derived from AbstractActiveQConsumerModule has to implement this method
   * so that it knows what type of message it is going to send to Malhar.
   *
   * @param message
   */
  protected abstract void emitMessage(Message message);

  @Override
  public void setup(OperatorConfiguration config)
  {
    try {
      //System.out.println("setup got called");
      activeMQHelper.setup(config);
      activeMQHelper.getConsumer().setMessageListener(this);

      maxMessages = activeMQHelper.getMaximumMessage();
    }
    catch (JMSException ex) {
      java.util.logging.Logger.getLogger(AbstractActiveMQConsumerModule.class.getName()).log(Level.SEVERE, null, ex);
    }

  }

  @Override
  public void teardown()
  {
    activeMQHelper.teardown();
  }

  @Override
  public synchronized void run()  // Do we need Synchronized ??
  {
    try {
      this.wait();
    }
    catch (InterruptedException ex) {
      logger.info("{} exiting generation of the input since got interrupted with {}", this, ex);
    }
  }

  /**
   * Whenever there is message available this will get called.
   * This just emit the message to Malhar platform.
   * @param message
   */
  @Override
  public void onMessage(Message message)
  {
    /**
     * make sure that we do not get called again if we have processed enough
     * messages already.
     */
    //logger.info("{} in call onMessage", this);
    if (maxMessages > 0) {
      if (--maxMessages == 0) {
        try {
          activeMQHelper.getConsumer().setMessageListener(null);
        }
        catch (JMSException ex) {
          logger.error(null, ex);
        }
      }
    }

    emitMessage(message);
    activeMQHelper.handleMessage(message);
  }

  @Override
  public void onException(JMSException jmse)
  {
    logger.error("Exception thrown by ActiveMQ consumer setup.", jmse.getCause());
  }
}
