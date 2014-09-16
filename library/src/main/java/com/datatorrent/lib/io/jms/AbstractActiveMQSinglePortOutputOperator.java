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

import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.api.annotation.InputPortFieldAnnotation;

import javax.jms.JMSException;
import javax.jms.Message;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This is an abstract ouput operator, which emits data to an ActiveMQ message bus.&nbsp;This operator has a single input port.
 *
 * <br>
 * Ports:<br>
 * <b>Input</b>: Have only one input port<br>
 * <b>Output</b>: No output port<br>
 * <br>
 * Properties:<br>
 * None<br>
 * <br>
 * Compile time checks:<br>
 * Class derived from this has to implement the abstract method createMessage() <br>
 * <br>
 * Run time checks:<br>
 * None<br>
 * <br>
 * Benchmarks:<br>
 * TBD<br>
 * <br>
 *
 * @displayName Abstract Active MQ Single Port Output
 * @category io
 * @tags jms, output
 *
 * @since 0.3.2
 */
public abstract class AbstractActiveMQSinglePortOutputOperator<T> extends AbstractActiveMQOutputOperator
{
  private static final Logger logger = LoggerFactory.getLogger(AbstractActiveMQSinglePortOutputOperator.class);
  long countMessages = 0;  // Number of messages produced so far

  /**
   * Convert tuple into JMS message. Tuple can be any Java Object.
   * @param tuple
   * @return Message
   */
  protected abstract Message createMessage(T tuple);

  /**
   * Convert to and send message.
   * @param tuple
   */
  protected void processTuple(T tuple)
  {
    countMessages++;

    if (countMessages > maxSendMessage && maxSendMessage != 0) {
      if (countMessages == maxSendMessage + 1) {
        logger.warn("Reached maximum send messages of {}", maxSendMessage);
      }
      return; // Stop sending messages after max limit.
    }

    try {
      Message msg = createMessage(tuple);
      getProducer().send(msg);
      //logger.debug("process message {}", tuple.toString());
    }
    catch (JMSException ex) {
      throw new RuntimeException("Failed to send message", ex);
    }
  }

  /**
   * The single input port.
   */
  @InputPortFieldAnnotation(name = "ActiveMQInputPort")
  public final transient DefaultInputPort<T> inputPort = new DefaultInputPort<T>()
  {
    @Override
    public void process(T tuple)
    {
      processTuple(tuple);
    }
  };
}
