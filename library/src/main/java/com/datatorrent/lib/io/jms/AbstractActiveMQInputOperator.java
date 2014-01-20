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
package com.datatorrent.lib.io.jms;

import com.datatorrent.api.ActivationListener;
import com.datatorrent.api.InputOperator;
import com.datatorrent.api.Context.OperatorContext;

import java.util.concurrent.ArrayBlockingQueue;
import javax.jms.JMSException;
import javax.jms.Message;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * ActiveMQ input adapter operator, which consume data from ActiveMQ message bus.<p><br>
 *
 * It uses PUSH model to get message. When there is a message available in AMQ message bus,
 * onMessage() got called which buffer the message into a holding buffer. At the same time
 * Malhar Streaming Platform calls emitTuples() on this operator to process message from
 * the holding buffer through output port. Output port simply emit the message to next
 * connected operator.
 * This class can be used if operator has more than one output ports. If it has only one
 * output port it can conveniently derived from AbstractActiveMQSinglePortInputOperator class.
 *
 * <br>
 * Ports:<br>
 * <b>Input</b>: No input port<br>
 * <b>Output</b>: Can have any number of output ports<br>
 * <br>
 * Properties:<br>
 * <b>tuplesBlast</b>: Number of tuples emitted in each burst<br>
 * <b>bufferSize</b>: Size of holding buffer<br>
 * <br>
 * Compile time checks:<br>
 * Class derived from this has to implement the abstract method emitTuple() <br>
 * <br>
 *
 * @since 0.3.2
 */
public abstract class AbstractActiveMQInputOperator extends ActiveMQConsumerBase implements InputOperator, ActivationListener<OperatorContext>
{
  private static final Logger logger = LoggerFactory.getLogger(AbstractActiveMQInputOperator.class);
  protected static final int TUPLES_BLAST_DEFAULT = 10 * 1024; // 10k
  protected static final int BUFFER_SIZE_DEFAULT = 1024 * 1024; // 1M
  // Config parameters that user can set.-
  private int tuplesBlast = TUPLES_BLAST_DEFAULT;
  private int bufferSize = BUFFER_SIZE_DEFAULT;
  protected transient ArrayBlockingQueue<Message> holdingBuffer;

  /**
   * Any concrete class derived from AbstractActiveMQInputOperator has to implement this method
   * so that it knows what type of message it is going to send to Malhar in which output port.
   *
   * @param message- the message to emit
   */
  protected abstract void emitTuple(Message message);

  /**
   * @return the tuplesBlast
   */
  public int getTuplesBlast()
  {
    return tuplesBlast;
  }

  /**
   * Sets the number of tuples emitted in each burst.
   * 
   * @param tuplesBlast the number of tuples to emit in each burst.
   */
  public void setTuplesBlast(int tuplesBlast)
  {
    this.tuplesBlast = tuplesBlast;
  }

  /**
   * @return the buffer size
   */
  public int getBufferSize()
  {
    return bufferSize;
  }
  
  /**
   * Sets the size of holding buffer.
   * 
   * @param bufferSize- the size of the holding buffer
   */
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
   * @param context
   */
  @Override
  public void setup(OperatorContext context)
  {
    holdingBuffer = new ArrayBlockingQueue<Message>(bufferSize);
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
    try {
      setupConnection();
    }
    catch (JMSException ex) {
      logger.error(ex.getLocalizedMessage());
    }
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
   * Pull message from temporary buffer and pass in emitTuple() in order to be processed/consumed
   * or pass to next operator.
   * To be more efficient emit all the tuples in buffer upto blastSize.
   * It also acknowledge that all messages have been received.
   */
  @Override
  public void emitTuples()
  {
    int messageCount = getTuplesBlast() < holdingBuffer.size() ? getTuplesBlast() : holdingBuffer.size();
    while (messageCount > 1) {
      emitTuple(holdingBuffer.poll());
      messageCount--;
    }

    // Acknowledge all message with last message in buffer.
    if (messageCount == 1) {
      Message msg = holdingBuffer.poll();
      emitTuple(msg);
      acknowledgeMessage(msg);
    }
  }
}
