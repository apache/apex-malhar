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

import javax.jms.JMSException;
import javax.jms.MessageProducer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Base class for any ActiveMQ output adapter operator. <p><br>
 * Output Operators should not be derived from this,
 * rather from AbstractActiveMQOutputOperator or AbstractActiveMQSinglePortOutputOperator. It creates the producer
 * to send message into active MQ message bus.<br>
 *
 * <br>
 * Ports:<br>
 * None<br>
 * <br>
 * Properties:<br>
 * None<br>
 * <br>
 * Compile time checks:<br>
 * None<br>
 * <br>
 * Run time checks:<br>
 * None<br>
 * <br>
 * Benchmarks:<br>
 * NA<br>
 * <br>
 *
 * @since 0.3.2
 */
public class ActiveMQProducerBase extends ActiveMQBase
{
  private static final Logger logger = LoggerFactory.getLogger(ActiveMQProducerBase.class);
  private transient MessageProducer producer;

  // Config parameters that user can set.
  private long maximumSendMessages = 0; // 0 means unlimitted

  /**
   * @return the message producer 
   */
  public MessageProducer getProducer()
  {
    return producer;
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
   *  Connection specific setup for ActiveMQ.
   *
   *  @throws JMSException
   */
  public void setupConnection() throws JMSException
  {
    super.createConnection();
    // Create producer
    producer = getSession().createProducer(getDestination());
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
}
