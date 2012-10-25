/*
 *  Copyright (c) 2012 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.malhartech.lib.io;

import com.malhartech.annotation.OutputPortFieldAnnotation;
import com.malhartech.api.AsyncInputOperator;
import com.malhartech.api.BaseOperator;
import com.malhartech.api.DefaultOutputPort;
import com.malhartech.api.OperatorConfiguration;
import java.util.ArrayList;
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
 * This is ActiveMQ input adapter operator (which consume data from ActiveMQ message bus).
 */
public abstract class AbstractActiveMQInputOperator<T> extends BaseOperator implements AsyncInputOperator, Runnable, MessageListener, ExceptionListener
{
  private static final Logger logger = LoggerFactory.getLogger(AbstractActiveMQInputOperator.class);
  private long maxMessages;
  public ActiveMQHelper activeMQHelper = new ActiveMQHelper(false);

  @OutputPortFieldAnnotation(name = "ActiveMQOutputPort")
  final public transient DefaultOutputPort<T> outputPort = new DefaultOutputPort<T>(this);
  private final transient ArrayList<T> tuples = new ArrayList<T>();

  @Override
  public void injectTuples(long windowId)
  {
    synchronized (tuples) {
      for (T tuple: tuples) {
        outputPort.emit(tuple);
      }
    }
  }

  /**
   * Any concrete class derived from AbstractActiveQConsumerModule has to implement this method
   * so that it knows what type of message it is going to send to Malhar.
   *
   * @param message
   */
    protected abstract T getTuple(Message message) throws JMSException;

  @Override
  public void setup(OperatorConfiguration config)
  {
    try {
      //System.out.println("setup got called");
      activeMQHelper.setup(config);
      activeMQHelper.getConsumer().setMessageListener(this);

      maxMessages = activeMQHelper.getMaximumMessage();
    }
    catch (JMSException jmse) {
      logger.error("Exception thrown by ActiveMQ consumer setup.", jmse.getCause());
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
   *
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

    synchronized (tuples) {
      try {
        tuples.add(getTuple(message));
      }
      catch (JMSException ex) {
        java.util.logging.Logger.getLogger(AbstractActiveMQInputOperator.class.getName()).log(Level.SEVERE, null, ex);
      }
    }

    activeMQHelper.handleMessage(message);
  }

  @Override
  public void onException(JMSException jmse)
  {
    logger.error("Exception thrown by ActiveMQ consumer.", jmse.getCause());
  }
}
