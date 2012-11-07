/*
 *  Copyright (c) 2012 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.malhartech.lib.io;

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
 * @author Locknath Shil <locknath@malhar-inc.com>
 *
 * This is ActiveMQ input adapter operator (which consume data from ActiveMQ message bus).
 */
public abstract class AbstractActiveMQInputOperator extends ActiveMQConsumerBase implements InputOperator, ActivationListener<OperatorContext>
{
  private static final Logger logger = LoggerFactory.getLogger(AbstractActiveMQInputOperator.class);
  protected static final int TUPLES_BLAST_DEFAULT = 10 * 1024; // 10k
  protected static final int BUFFER_SIZE_DEFAULT = 1024 * 1024; // 1M
  // Config parameters that user can set.-
  private int tuplesBlast = TUPLES_BLAST_DEFAULT;
  private int bufferSize = BUFFER_SIZE_DEFAULT;
  protected transient CircularBuffer<Message> holdingBuffer = new CircularBuffer<Message>(bufferSize); // Should this be transient?

  /**
   * Any concrete class derived from AbstractActiveMQInputOperator has to implement this method
   * so that it knows what type of message it is going to send to Malhar in which output port.
   *
   * @param message
   */
  protected abstract void emitTuple(Message message);

  public int getTuplesBlast()
  {
    return tuplesBlast;
  }

  public void setTuplesBlast(int tuplesBlast)
  {
    this.tuplesBlast = tuplesBlast;
  }

  public int getBufferSize()
  {
    return bufferSize;
  }

  public void setBufferSize(int bufferSize)
  {
    this.bufferSize = bufferSize;
  }

  /**
   * Implement abstract method of ActiveMQConsumerBase
   */
  @Override
  protected final void emitMessage(Message message) throws JMSException
  {
    holdingBuffer.add(message);
  }

  /**
   * Implement Component Interface.
   *
   * @param config
   */
  @Override
  public void setup(OperatorConfiguration config)
  {
  }

  /**
   * Implement Component Interface.
   */
  @Override
  public void teardown()
  {
  }

  /**
   * Implement Operator Interface.
   */
  @Override
  public void beginWindow(long windowId)
  {
  }

  /**
   * Implement Operator Interface.
   */
  @Override
  public void endWindow()
  {
  }

  /**
   * Implement ActivationListener Interface.
   */
  @Override
  public void postActivate(OperatorContext ctx)
  {
    try {
      setupConnection();
    }
    catch (JMSException ex) {
      logger.debug(ex.getLocalizedMessage());
    }
  }

  /**
   * Implement ActivationListener Interface.
   */
  @Override
  public void preDeactivate()
  {
    cleanup();
  }

  /**
   * Implement InputOperator Interface.
   */
  @Override
  public void emitTuples()
  {
    int messageCount = getTuplesBlast() < holdingBuffer.size() ? getTuplesBlast() : holdingBuffer.size();
    while (messageCount > 1) {
      emitTuple(holdingBuffer.pollUnsafe());
      messageCount--;
    }

    // Acknowledge all message with last message in buffer.
    if (messageCount == 1) {
      Message msg = holdingBuffer.pollUnsafe();
      emitTuple(msg);
      acknowledgeMessage(msg);
    }
  }
}
