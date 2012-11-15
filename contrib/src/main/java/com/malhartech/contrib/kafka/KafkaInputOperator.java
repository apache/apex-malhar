/*
 *  Copyright (c) 2012 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.malhartech.contrib.kafka;

import com.malhartech.api.ActivationListener;
import com.malhartech.api.Context.OperatorContext;
import com.malhartech.api.InputOperator;
import com.malhartech.util.CircularBuffer;
import kafka.message.Message;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class KafkaInputOperator extends KafkaBase implements InputOperator, ActivationListener<OperatorContext>
{
  private static final Logger logger = LoggerFactory.getLogger(KafkaInputOperator.class);
  protected static final int TUPLES_BLAST_DEFAULT = 10 * 1024; // 10k
  protected static final int BUFFER_SIZE_DEFAULT = 1024 * 1024; // 1M
  // Config parameters that user can set.-
  private int tuplesBlast = TUPLES_BLAST_DEFAULT;
  private int bufferSize = BUFFER_SIZE_DEFAULT;
  protected transient CircularBuffer<Message> holdingBuffer = new CircularBuffer<Message>(bufferSize);

  /**
   * Any concrete class derived from KafkaInputOperator has to implement this method
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
  public final void emitMessage(Message message)
  {
    holdingBuffer.add(message);
  }

  /**
   * Implement Component Interface.
   *
   * @param config
   */
  @Override
  public void setup(OperatorContext context)
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
  public void activate(OperatorContext ctx)
  {
    //createSimpleConsumer();
    //simpleConsumerOnMessage();

    createConsumer("topic1");
    onMessage();

  }

  /**
   * Implement ActivationListener Interface.
   */
  @Override
  public void deactivate()
  {
    cleanup();
  }

  /**
   * Implement InputOperator Interface.
   */
  @Override
  public void emitTuples()
  {
    int bufferLength = holdingBuffer.size();
    for (int i = getTuplesBlast() < bufferLength ? getTuplesBlast() : bufferLength; i-- > 0;) {
      Message msg = holdingBuffer.pollUnsafe();
      emitTuple(msg);
      //logger.debug("emitTuples() got called from {} with tuple: {}", this, msg);
    }
  }
}
