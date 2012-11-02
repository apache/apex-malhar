/*
 *  Copyright (c) 2012 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.malhartech.lib.io;

import com.malhartech.annotation.InjectConfig;
import com.malhartech.api.ActivationListener;
import com.malhartech.api.Context.OperatorContext;
import com.malhartech.api.InputOperator;
import com.malhartech.util.CircularBuffer;
import javax.jms.JMSException;
import javax.jms.Message;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 *  @author Locknath Shil <locknath@malhar-inc.com>
 *  This is ActiveMQ input adapter operator (which consume data from ActiveMQ message bus).
 */
public abstract class AbstractActiveMQInputOperator<T> extends ActiveMQConsumerBase implements InputOperator, ActivationListener<OperatorContext>
{
  private static final Logger logger = LoggerFactory.getLogger(AbstractActiveMQInputOperator.class);
  private final transient int bufferSize = 1024 * 1024;
  protected transient CircularBuffer<T> holdingBuffer = new CircularBuffer<T>(bufferSize); // Should this be transient?
  protected static final int TUPLES_BLAST_DEFAULT = 10000;
  // Config parameters that user can set.
  @InjectConfig(key = "tuplesBlast")
  private int tuplesBlast = TUPLES_BLAST_DEFAULT;

  /**
   *  Any concrete class derived from AbstractActiveQConsumerModule has to implement this method
   *  so that it knows what type of message it is going to send to Malhar.
   *  It converts a JMS message into a Tuple. A Tuple can be of any type (derived from Java Object) that
   *  operator user intends to.
   *
   *  @param message
   */
  protected abstract T getTuple(Message message) throws JMSException;

  public int getTuplesBlast()
  {
    return tuplesBlast;
  }

  public void setTuplesBlast(int tuplesBlast)
  {
    this.tuplesBlast = tuplesBlast;
  }

  /**
   *  Implement abstract method of ActiveMQConsumerBase
   */
  @Override
  protected void emitMessage(Message message) throws JMSException
  {
    T tuple = getTuple(message);
    holdingBuffer.add(tuple); // do we need to check buffer capacity?? TBD
  }

  /**
   *  Implement Component Interface.
   *
   *  @param config
   */
  @Override
  public void setup(OperatorConfiguration config)
  {
    logger.debug("setup got called from {}", this);
  }

  /**
   *  Implement Component Interface.
   */
  @Override
  public void teardown()
  {
    logger.debug("teardown got called from {}", this);
    // cleanup(); TBD
  }

  /**
   *  Implement Operator Interface.
   */
  @Override
  public void beginWindow(long windowId)
  {
    logger.debug("beginWindow got called from {}", this);
  }

  /**
   *  Implement Operator Interface.
   */
  @Override
  public void endWindow()
  {
    logger.debug("endWindow got called from {}", this);
  }

  /**
   *  Implement ActivationListener Interface.
   */
  @Override
  public void postActivate(OperatorContext ctx)
  {
    logger.debug("postActivate got called from {}", this);
    try {
      setupConnection();
    }
    catch (JMSException ex) {
      logger.debug(ex.getLocalizedMessage());
    }
  }

  /**
   *  Implement ActivationListener Interface.
   */
  @Override
  public void preDeactivate()
  {
    logger.debug("preDeactivate got called from {}", this);
  }
}
