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
package org.apache.apex.malhar.solace;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.TimeUnit;
import javax.validation.constraints.NotNull;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Maps;
import com.solacesystems.jcsmp.BytesXMLMessage;
import com.solacesystems.jcsmp.JCSMPException;
import com.solacesystems.jcsmp.Topic;
import com.datatorrent.api.Context;
import com.datatorrent.api.InputOperator;
import com.datatorrent.api.Operator;
import com.datatorrent.netlet.util.DTThrowable;

public abstract class AbstractSolaceDirectInputOperator<T> extends AbstractSolaceBaseInputOperator<T> implements InputOperator, Operator.ActivationListener<com.datatorrent.api.Context.OperatorContext>
{

  private static final Logger LOG = LoggerFactory.getLogger(AbstractSolaceDirectInputOperator.class);

  @NotNull
  protected String topicName;
  private transient Topic topic;
  //private transient BytesXMLMessage recentMessage = null;

  //private transient List<T> messages = new ArrayList<T>();
  protected final transient Map<Long, T> currentWindowRecoveryState = Maps.newLinkedHashMap();

  //private final transient AtomicReference<Throwable> throwable;

  @Override
  public void setup(Context.OperatorContext context)
  {
    arrivedTopicMessagesToProcess = new ArrayBlockingQueue<BytesXMLMessage>(this.unackedMessageLimit);

    //setup info for HA and DR at the transport level
    super.setReapplySubscriptions(true);

    super.setup(context);
  }

  public AbstractSolaceDirectInputOperator()
  {
    //throwable = new AtomicReference<Throwable>();
  }

  @Override
  public void activate(Context.OperatorContext context)
  {
    super.activate(context);
    topic = factory.createTopic(topicName);
    addSubscription(topic);
  }

  @Override
  public void beginWindow(long windowId)
  {
    super.beginWindow(windowId);
    if (windowId <= lastCompletedWId) {
      handleRecovery(currentWindowId);
    }
  }

  @Override
  public void endWindow()
  {
    super.endWindow();
    try {
      if (currentWindowId > lastCompletedWId) {
        idempotentStorageManager.save(currentWindowRecoveryState, operatorId, currentWindowId);
      }
      currentWindowRecoveryState.clear();
    } catch (IOException e) {
      DTThrowable.rethrow(e);
    }
    emitCount = 0; //reset emit count
  }

  @Override
  public void emitTuples()
  {
    if (throwable != null) {
      DTThrowable.rethrow(throwable);
    }

    if (currentWindowId <= idempotentStorageManager.getLargestRecoveryWindow()) {
      return;
    }

    BytesXMLMessage message;
    try {
      while (emitCount < DEFAULT_BUFFER_SIZE && ((message = (BytesXMLMessage)super.arrivedTopicMessagesToProcess.poll(10, TimeUnit.MILLISECONDS)) != null)) {
        processMessage(message);
        emitCount++;
      }
    } catch (InterruptedException e) {
      DTThrowable.rethrow(e);
    }
    //emitCount++;
  }

  protected void addSubscription(Topic topic)
  {
    try {
      session.addSubscription(topic);
    } catch (JCSMPException e) {
      DTThrowable.rethrow(e);
    }
  }

  protected void removeSubscription(Topic topic)
  {
    try {
      session.removeSubscription(topic);
    } catch (JCSMPException e) {
      DTThrowable.rethrow(e);
    }
  }

  /*
  @Override
  protected T processMessage(BytesXMLMessage message)
  {
      T tuple = super.processMessage(message);
      if(tuple != null) {
          messages.add(tuple);
          return tuple;
      }

      return null;
  }
  */

  @Override
  protected T processMessage(BytesXMLMessage message)
  {
    T payload = super.processMessage(message);
    if (payload != null) {
      currentWindowRecoveryState.put(message.getMessageIdLong(), payload);
    }
    //recentMessage = message;
    return payload;
  }

  @SuppressWarnings("unchecked")
  protected void handleRecovery(long windowId)
  {
    LOG.info("Handle Recovery called {}", windowId);

    Map<Long, T> recoveredData;
    try {
      recoveredData = (Map<Long, T>)idempotentStorageManager.load(operatorId, windowId);

      if (recoveredData == null) {
        return;
      }
      for (Map.Entry<Long, T> recoveredEntry : recoveredData.entrySet()) {
        emitTuple(recoveredEntry.getValue());
      }

    } catch (IOException e) {
      DTThrowable.rethrow(e);
    }
  }

  public String getTopicName()
  {
    return topicName;
  }

  public void setTopicName(String topicName)
  {
    this.topicName = topicName;
  }

}
