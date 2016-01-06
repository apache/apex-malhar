/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package com.datatorrent.lib.io.jms;

import java.io.IOException;
import java.util.Arrays;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.atomic.AtomicReference;

import javax.jms.ExceptionListener;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageListener;
import javax.jms.MessageProducer;
import javax.jms.Session;
import javax.jms.Topic;
import javax.validation.constraints.Min;
import javax.validation.constraints.NotNull;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.commons.lang.mutable.MutableLong;
import org.apache.hadoop.classification.InterfaceStability;

import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

import com.datatorrent.api.Context;
import com.datatorrent.api.Context.OperatorContext;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.api.InputOperator;
import com.datatorrent.api.Operator;
import com.datatorrent.api.Operator.ActivationListener;
import com.datatorrent.api.annotation.OperatorAnnotation;
import com.datatorrent.lib.counters.BasicCounters;
import com.datatorrent.lib.io.IdempotentStorageManager;
import com.datatorrent.netlet.util.DTThrowable;

/**
 * This is the base implementation of a JMS input operator.<br/>
 * Subclasses must implement the method that converts JMS messages into tuples for emission.
 * <p/>
 * The operator acts as a listener for JMS messages. When there is a message available in the message bus,
 * {@link #onMessage(Message)} is called which buffers the message into a holding buffer. This is asynchronous.<br/>
 * {@link #emitTuples()} retrieves messages from holding buffer and processes them.
 * <p/>
 * Important: The {@link IdempotentStorageManager.FSIdempotentStorageManager} makes the operator fault tolerant as
 * well as idempotent. If {@link IdempotentStorageManager.NoopIdempotentStorageManager} is set on the operator then
 * it will not be fault-tolerant as well.
 * <p/>
 * Configurations:<br/>
 * <b>bufferSize</b>: Controls the holding buffer size.<br/>
 * <b>consumerName</b>: Name that identifies the subscription.<br/>
 *
 * @param <T> type of tuple emitted
 * @displayName Abstract JMS Input
 * @category Messaging
 * @tags jms, input operator
 * @since 0.3.2
 */
@InterfaceStability.Stable
@OperatorAnnotation(checkpointableWithinAppWindow = false)
public abstract class AbstractJMSInputOperator<T> extends JMSBase
    implements InputOperator, ActivationListener<OperatorContext>, MessageListener, ExceptionListener,
    Operator.IdleTimeHandler, Operator.CheckpointListener
{
  protected static final int DEFAULT_BUFFER_SIZE = 10 * 1024; // 10k

  //Configurations:
  @Min(1)
  protected int bufferSize = DEFAULT_BUFFER_SIZE;
  private String consumerName;

  protected transient ArrayBlockingQueue<Message> holdingBuffer;
  protected final transient Map<String, T> currentWindowRecoveryState;

  protected transient Message lastMsg;

  private transient MessageProducer replyProducer;
  private transient MessageConsumer consumer;

  @NotNull
  private final BasicCounters<MutableLong> counters;
  private transient Context.OperatorContext context;
  private transient long spinMillis;

  private final transient AtomicReference<Throwable> throwable;

  @NotNull
  protected IdempotentStorageManager idempotentStorageManager;
  private transient long[] operatorRecoveredWindows;
  protected transient long currentWindowId;
  protected transient int emitCount;

  private final transient Set<String> pendingAck;
  private final transient Lock lock;

  public final transient DefaultOutputPort<T> output = new DefaultOutputPort<T>();

  public AbstractJMSInputOperator()
  {
    counters = new BasicCounters<MutableLong>(MutableLong.class);
    throwable = new AtomicReference<Throwable>();
    pendingAck = Sets.newHashSet();
    idempotentStorageManager = new IdempotentStorageManager.FSIdempotentStorageManager();

    lock = new Lock();

    //Recovery state is a linked hash map to maintain the order of tuples.
    currentWindowRecoveryState = Maps.newLinkedHashMap();
    holdingBuffer = new ArrayBlockingQueue<Message>(bufferSize)
    {
      private static final long serialVersionUID = 201411151139L;

      @SuppressWarnings("Contract")
      @Override
      public boolean add(Message message)
      {
        synchronized (lock) {
          try {
            return messageConsumed(message) && super.add(message);
          } catch (JMSException e) {
            LOG.error("message consumption", e);
            throwable.set(e);
            throw new RuntimeException(e);
          }
        }
      }
    };
  }

  /**
   * Implementation of {@link MessageListener} interface.<br/>
   * Whenever there is message available in the message bus this will get called.
   *
   * @param message
   */
  @Override
  public final void onMessage(Message message)
  {
    holdingBuffer.add(message);
    sendReply(message);
  }

  /**
   * If getJMSReplyTo is set then send message back to reply producer.
   *
   * @param message
   */
  protected void sendReply(Message message)
  {
    try {
      if (message.getJMSReplyTo() != null) { // Send reply only if the replyTo destination is set
        replyProducer.send(message.getJMSReplyTo(),
            getSession().createTextMessage("Reply: " + message.getJMSMessageID()));
      }
    } catch (JMSException ex) {
      LOG.error(ex.getLocalizedMessage());
      throwable.set(ex);
      throw new RuntimeException(ex);
    }
  }

  /**
   * Implementation of {@link ExceptionListener}
   *
   * @param ex
   */
  @Override
  public void onException(JMSException ex)
  {
    cleanup();
    LOG.error(ex.getLocalizedMessage());
    throwable.set(ex);
    throw new RuntimeException(ex);
  }

  @Override
  public void setup(OperatorContext context)
  {
    this.context = context;
    spinMillis = context.getValue(OperatorContext.SPIN_MILLIS);
    counters.setCounter(CounterKeys.RECEIVED, new MutableLong());
    counters.setCounter(CounterKeys.REDELIVERED, new MutableLong());
    idempotentStorageManager.setup(context);
    try {
      operatorRecoveredWindows = idempotentStorageManager.getWindowIds(context.getId());
      if (operatorRecoveredWindows != null) {
        Arrays.sort(operatorRecoveredWindows);
      }
    } catch (IOException e) {
      throw new RuntimeException("fetching windows", e);
    }
  }

  /**
   * This method is called when a message is added to {@link #holdingBuffer} and can be overwritten by subclasses
   * if required. This is called by the JMS thread not Operator thread.
   *
   * @param message
   * @return message is accepted.
   * @throws javax.jms.JMSException
   */
  protected boolean messageConsumed(Message message) throws JMSException
  {
    if (message.getJMSRedelivered() && pendingAck.contains(message.getJMSMessageID())) {
      counters.getCounter(CounterKeys.REDELIVERED).increment();
      LOG.warn("IGNORING: Redelivered Message {}", message.getJMSMessageID());
      return false;
    }
    pendingAck.add(message.getJMSMessageID());
    MutableLong receivedCt = counters.getCounter(CounterKeys.RECEIVED);
    receivedCt.increment();
    LOG.debug("message id: {} buffer size: {} received: {}", message.getJMSMessageID(), holdingBuffer.size(),
        receivedCt.longValue());
    return true;
  }

  /**
   * Implement ActivationListener Interface.
   * @param ctx
   */
  @Override
  public void activate(OperatorContext ctx)
  {
    try {
      super.createConnection();
      replyProducer = getSession().createProducer(null);

      consumer = (isDurable() && isTopic()) ?
          getSession().createDurableSubscriber((Topic)getDestination(), consumerName) :
          getSession().createConsumer(getDestination());
      consumer.setMessageListener(this);
    } catch (JMSException ex) {
      throw new RuntimeException(ex);
    }
  }

  /**
   * Implementation of {@link Operator} interface.
   */
  @Override
  public void beginWindow(long windowId)
  {
    currentWindowId = windowId;
    if (windowId <= idempotentStorageManager.getLargestRecoveryWindow()) {
      replay(windowId);
    }
  }

  protected void replay(long windowId)
  {
    try {
      @SuppressWarnings("unchecked")
      Map<String, T> recoveredData = (Map<String, T>)idempotentStorageManager.load(context.getId(), windowId);
      if (recoveredData == null) {
        return;
      }
      for (Map.Entry<String, T> recoveredEntry : recoveredData.entrySet()) {
        pendingAck.add(recoveredEntry.getKey());
        emit(recoveredEntry.getValue());
      }
    } catch (IOException e) {
      throw new RuntimeException("replay", e);
    }
  }

  @Override
  public void emitTuples()
  {
    if (currentWindowId <= idempotentStorageManager.getLargestRecoveryWindow()) {
      return;
    }

    Message msg;
    while (emitCount < bufferSize && (msg = holdingBuffer.poll()) != null) {
      processMessage(msg);
      emitCount++;
      lastMsg = msg;
    }
  }

  /**
   * Process jms message.
   *
   * @param message
   */
  protected void processMessage(Message message)
  {
    try {
      T payload = convert(message);
      if (payload != null) {
        currentWindowRecoveryState.put(message.getJMSMessageID(), payload);
        emit(payload);
      }
    } catch (JMSException e) {
      throw new RuntimeException("processing msg", e);
    }
  }

  @Override
  public void handleIdleTime()
  {
    Throwable lthrowable = throwable.get();
    if (lthrowable == null) {
      /* nothing to do here, so sleep for a while to avoid busy loop */
      try {
        Thread.sleep(spinMillis);
      } catch (InterruptedException ie) {
        throw new RuntimeException(ie);
      }
    } else {
      DTThrowable.rethrow(lthrowable);
    }
  }

  /**
   * JMS API has a drawback that it only allows acknowledgement/commitment of all the messages which have been consumed
   * in a session instead of all the messages received till a particular message.<br/>
   *
   * This creates complications with recovery/idempotency as we need to ensure that the messages that are being
   * acknowledged have been persisted because they wouldn't be redelivered. Also if they are persisted then
   * they shouldn't be re-delivered because that would cause duplicates.<br/>
   *
   * This is why when recovery data is persisted and messages are acknowledged, the thread that consumes message is
   * blocked.<br/>
   */
  @Override
  public void endWindow()
  {
    if (currentWindowId > idempotentStorageManager.getLargestRecoveryWindow()) {
      synchronized (lock) {
        boolean stateSaved = false;
        boolean ackCompleted = false;
        try {
          //No more messages can be consumed now. so we will call emit tuples once more
          //so that any pending messages can be emitted.
          Message msg;
          while ((msg = holdingBuffer.poll()) != null) {
            processMessage(msg);
            emitCount++;
            lastMsg = msg;
          }
          idempotentStorageManager.save(currentWindowRecoveryState, context.getId(), currentWindowId);
          stateSaved = true;

          currentWindowRecoveryState.clear();
          if (lastMsg != null) {
            acknowledge();
          }
          ackCompleted = true;
          pendingAck.clear();
        } catch (Throwable t) {
          if (!ackCompleted) {
            LOG.info("confirm recovery of {} for {} does not exist", context.getId(), currentWindowId, t);
          }
          DTThrowable.rethrow(t);
        } finally {
          if (stateSaved && !ackCompleted) {
            try {
              idempotentStorageManager.delete(context.getId(), currentWindowId);
            } catch (IOException e) {
              LOG.error("unable to delete corrupted state", e);
            }
          }
        }
      }
      emitCount = 0; //reset emit count
    } else if (operatorRecoveredWindows != null &&
        currentWindowId < operatorRecoveredWindows[operatorRecoveredWindows.length - 1]) {
      //pendingAck is not cleared for the last replayed window of this operator. This is because there is
      //still a chance that in the previous run the operator crashed after saving the state but before acknowledgement.
      pendingAck.clear();
    }

    context.setCounters(counters);
  }

  /**
   * Commit/Acknowledge messages that have been received.<br/>
   * @throws javax.jms.JMSException
   */
  protected void acknowledge() throws JMSException
  {
    if (isTransacted()) {
      getSession().commit();
    } else if (getSessionAckMode(getAckMode()) == Session.CLIENT_ACKNOWLEDGE) {
      lastMsg.acknowledge(); // acknowledge all consumed messages till now
    }
  }

  @Override
  public void checkpointed(long windowId)
  {
  }

  @Override
  public void committed(long windowId)
  {
    try {
      idempotentStorageManager.deleteUpTo(context.getId(), windowId);
    } catch (IOException e) {
      throw new RuntimeException("committing", e);
    }
  }

  @Override
  public void deactivate()
  {
    cleanup();
  }

  @Override
  protected void cleanup()
  {
    try {
      consumer.setMessageListener(null);
      replyProducer.close();
      replyProducer = null;
      consumer.close();
      consumer = null;

      super.cleanup();
    } catch (JMSException ex) {
      throw new RuntimeException("at cleanup", ex);
    }
  }

  @Override
  public void teardown()
  {
    idempotentStorageManager.teardown();
  }

  /**
   * Converts a {@link Message} to type T which is emitted.
   *
   * @param message
   * @return newly constructed tuple from the message.
   * @throws javax.jms.JMSException
   */
  protected abstract T convert(Message message) throws JMSException;

  /**
   * @return the bufferSize
   */
  public int getBufferSize()
  {
    return bufferSize;
  }

  /**
   * Sets the number of tuples emitted in each burst.
   *
   * @param bufferSize the number of tuples to emit in each burst.
   */
  public void setBufferSize(int bufferSize)
  {
    this.bufferSize = bufferSize;
  }

  /**
   * @return the consumer name
   */
  public String getConsumerName()
  {
    return consumerName;
  }

  /**
   * Sets the name for the consumer.
   *
   * @param consumerName- the name for the consumer
   */
  public void setConsumerName(String consumerName)
  {
    this.consumerName = consumerName;
  }

  /**
   * Sets this idempotent storage manager.
   *
   * @param storageManager
   */
  public void setIdempotentStorageManager(IdempotentStorageManager storageManager)
  {
    this.idempotentStorageManager = storageManager;
  }

  /**
   * @return the idempotent storage manager.
   */
  public IdempotentStorageManager getIdempotentStorageManager()
  {
    return this.idempotentStorageManager;
  }

  protected abstract void emit(T payload);

  public static enum CounterKeys
  {
    RECEIVED, REDELIVERED
  }

  private static class Lock
  {
  }

  private static final Logger LOG = LoggerFactory.getLogger(AbstractJMSInputOperator.class);
}
