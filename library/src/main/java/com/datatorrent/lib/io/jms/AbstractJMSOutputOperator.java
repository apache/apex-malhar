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

import com.datatorrent.api.Context.OperatorContext;
import com.datatorrent.api.DAG;
import com.datatorrent.api.Operator;
import com.google.common.collect.Lists;
import java.io.IOException;
import java.util.List;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageProducer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This is the base implementation of an ActiveMQ output operator.&nbsp;
 * A concrete operator should be created from this skeleton implementation.
 * <p>
 * This operator receives tuples from Malhar Streaming Platform through its input ports.
 * When the tuple is available in input ports it converts that to JMS message and send into
 * AMQ message bus. The concrete class of this has to implement the abstract method
 * how to convert tuple into JMS message.
 * </p>
 * Ports:<br>
 * <b>Input</b>: Can have any number of input ports<br>
 * <b>Output</b>: No output port<br>
 * <br>
 * </p>
 * @displayName Abstract Active MQ Output
 * @category Messaging
 * @tags jms, output operator
 *
 * @since 0.3.2
 */
public abstract class AbstractJMSOutputOperator extends JMSBase implements Operator
{
  private static final Logger logger = LoggerFactory.getLogger(AbstractJMSOutputOperator.class);
  long maxSendMessage = 0; // max send limit

  /**
   * Use this field to getStore() tuples from which messages are created.
   */
  private List<Object> tupleBatch = Lists.newArrayList();
  /**
   * Use this field to getStore() messages to be sent in batch.
   */
  private List<Message> messageBatch = Lists.newArrayList();

  private transient String appId;
  private transient int operatorId;
  private transient long committedWindowId;
  /**
   * The id of the current window.
   * Note this is not transient to handle the case that the operator restarts from a checkpoint that is
   * in the middle of the application window.
   */
  private long currentWindowId;
  private ProcessingMode mode;

  private transient MessageProducer producer;
  private JMSBaseTransactionableStore store = new JMSTransactionableStore();

  // Config parameters that user can set.
  private long maximumSendMessages = 0; // 0 means unlimitted

  /**
   * Implement Component Interface.
   *
   * @param context
   */
  @Override
  public void setup(OperatorContext context)
  {
    appId = context.getValue(DAG.APPLICATION_ID);
    operatorId = context.getId();

    logger.debug("Application Id {} operatorId {}", appId, operatorId);
    logger.debug("Store class {}", getStore().getClass());

    getStore().setBase(this);
    getStore().setAppId(appId);
    getStore().setOperatorId(operatorId);
    transacted = getStore().isTransactable();

    try {
      createConnection();
    }
    catch (JMSException ex) {
      logger.debug(ex.getLocalizedMessage());
      throw new RuntimeException(ex);
    }

    logger.debug("Session is null {}:", getSession() == null);

    try {
      getStore().connect();
    }
    catch (IOException ex) {
      throw new RuntimeException(ex);
    }

    logger.debug("Done connecting store.");

    mode = context.getValue(OperatorContext.PROCESSING_MODE);

    if(mode==ProcessingMode.AT_MOST_ONCE){
      //Batch must be cleared to avoid writing same data twice
      tupleBatch.clear();
    }

    for (Object tempObject: this.tupleBatch) {
      messageBatch.add(createMessage(tempObject));
    }

    committedWindowId = getStore().getCommittedWindowId(appId, operatorId);
    logger.debug("committedWindowId {}", committedWindowId);
    maxSendMessage = getMaximumSendMessages();

    logger.debug("End of setup store in transaction: {}", getStore().isInTransaction());
  }

  @Override
  public void teardown()
  {
    tupleBatch.clear();
    messageBatch.clear();

    logger.debug("beginning teardown");
    try {
      getStore().disconnect();
    }
    catch (IOException ex) {
      throw new RuntimeException(ex);
    }

    cleanup();

    logger.debug("ending teardown");
  }

  /**
   * Implement Operator Interface.
   */
  @Override
  public void beginWindow(long windowId)
  {
    currentWindowId = windowId;
    getStore().beginTransaction();
    logger.debug("Transaction started for window {}", windowId);
    logger.debug("Store class {}", getStore().getClass());
  }

  @Override
  public void endWindow()
  {
    logger.debug("Ending window {}", currentWindowId);

    if(getStore().isExactlyOnce()) {
      //Store committed window and data in same transaction
      if (committedWindowId < currentWindowId) {
        logger.debug("{}", getStore().getClass());
        getStore().storeCommittedWindowId(appId, operatorId, currentWindowId);
        committedWindowId = currentWindowId;
      }

      flushBatch();
      getStore().commitTransaction();
    }
    else {
      //For transactionable stores which cannot support exactly once, At least
      //once can be insured by for storing the data and then the committed window
      //id.
      flushBatch();
      getStore().commitTransaction();

      if (committedWindowId < currentWindowId) {
        logger.debug("{}", getStore().getClass());
        getStore().storeCommittedWindowId(appId, operatorId, currentWindowId);
        committedWindowId = currentWindowId;
      }
    }

    logger.debug("done ending window {}", currentWindowId);
  }

  /**
   * This is a helper method which flushes all the batched data.
   */
  protected void flushBatch()
  {
    logger.debug("flushing batch, batch size {}", tupleBatch.size());

    for (Message message: messageBatch) {
      try {
        producer.send(message);
      }
      catch (JMSException ex) {
        throw new RuntimeException(ex);
      }
    }

    tupleBatch.clear();
    messageBatch.clear();

    logger.debug("done flushing batch");
  }

  /**
   * This is a helper method which should be called to send a message.
   * @param data The data which will be converted into a message.
   */
  protected void sendMessage(Object data)
  {
    if(currentWindowId <= committedWindowId) {
      return;
    }

    tupleBatch.add(data);
    Message message = createMessage(data);
    messageBatch.add(message);

    if (tupleBatch.size() >= this.getBatch()) {
      flushBatch();
    }
  }

  public void setStore(JMSBaseTransactionableStore store)
  {
    logger.debug("Store set {}", store.getClass());
    this.store = store;
  }

  public JMSBaseTransactionableStore getStore()
  {
    return store;
  }

  /**
   * @return the maximum sent messages
   */
  public long getMaximumSendMessages()
  {
    return maximumSendMessages;
  }

  /**
   * Sets the maximum number of messages that can be sent.
   *
   * @param maximumSendMessages the max limit on messages sent
   */
  public void setMaximumSendMessages(long maximumSendMessages)
  {
    this.maximumSendMessages = maximumSendMessages;
  }

  /**
   *  Release resources.
   */
  @Override
  public void cleanup()
  {
    try {
      producer.close();
      producer = null;

      super.cleanup();
    }
    catch (JMSException ex) {
      logger.error(null, ex);
    }
  }

  /**
   *  Connection specific setup for ActiveMQ.
   *
   *  @throws JMSException
   */
  @Override
  public void createConnection() throws JMSException
  {
    super.createConnection();
    // Create producer
    producer = getSession().createProducer(getDestination());
  }

  /**
   * Convert tuple into JMS message. Tuple can be any Java Object.
   * @param tuple
   * @return Message
   */
  protected abstract Message createMessage(Object tuple);
}
