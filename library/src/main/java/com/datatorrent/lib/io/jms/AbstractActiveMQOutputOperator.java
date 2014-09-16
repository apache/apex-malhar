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

import com.datatorrent.api.Operator;
import com.datatorrent.api.Context.OperatorContext;

import javax.jms.JMSException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This is an abstract output operator, which emits data to an ActiveMQ message bus.
 *
 * This operator receives tuples from Malhar Streaming Platform through its input ports.
 * When the tuple is available in input ports it converts that to JMS message and send into
 * AMQ message bus. The concrete class of this has to implement the abstract method
 * how to convert tuple into JMS message.
 *
 * <br>
 * Ports:<br>
 * <b>Input</b>: Can have any number of input ports<br>
 * <b>Output</b>: No output port<br>
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
 * TBD<br>
 * <br>
 *
 * @displayName Abstract Active MQ Output
 * @category io
 * @tags jms, output
 *
 * @since 0.3.2
 */
public abstract class AbstractActiveMQOutputOperator extends ActiveMQProducerBase implements Operator
{
  private static final Logger logger = LoggerFactory.getLogger(AbstractActiveMQOutputOperator.class);
  long maxSendMessage = 0; // max send limit

  /**
   * Implement Component Interface.
   *
   * @param context
   */
  @Override
  public void setup(OperatorContext context)
  {
    try {
      setupConnection();
    }
    catch (JMSException ex) {
      logger.debug(ex.getLocalizedMessage());
    }
    maxSendMessage = getMaximumSendMessages();
  }

  /**
   * Implement Component Interface.
   */
  @Override
  public void teardown()
  {
    cleanup();
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
}
