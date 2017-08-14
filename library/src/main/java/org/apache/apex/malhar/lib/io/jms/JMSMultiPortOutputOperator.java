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
package org.apache.apex.malhar.lib.io.jms;

import java.io.Serializable;
import java.util.Map;

import javax.jms.BytesMessage;
import javax.jms.JMSException;
import javax.jms.MapMessage;
import javax.jms.Message;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.api.annotation.InputPortFieldAnnotation;

/**
 * @since 2.1.0
 */
@org.apache.hadoop.classification.InterfaceStability.Evolving
public class JMSMultiPortOutputOperator extends AbstractJMSOutputOperator
{
  /**
   * Convert to and send message.
   *
   * @param tuple
   */
  protected void processTuple(Object tuple)
  {
    sendMessage(tuple);
  }

  /**
   * This is an input port which receives map tuples to be written out to an JMS message bus.
   */
  @InputPortFieldAnnotation(optional = true)
  public final transient DefaultInputPort<Map<?,?>> inputMapPort = new DefaultInputPort<Map<?,?>>()
  {
    @Override
    public void process(Map<?,?> tuple)
    {
      sendMessage(tuple);
    }

  };

  /**
   * This is an input port which receives byte array tuples to be written out to an JMS message bus.
   */
  @InputPortFieldAnnotation(optional = true)
  public final transient DefaultInputPort<byte[]> inputByteArrayPort = new DefaultInputPort<byte[]>()
  {
    @Override
    public void process(byte[] tuple)
    {
      sendMessage(tuple);
    }

  };

  /**
   * This is an input port which receives Serializable tuples to be written out to an JMS message bus.
   */
  @InputPortFieldAnnotation(optional = true)
  public final transient DefaultInputPort<Serializable> inputObjectPort = new DefaultInputPort<Serializable>()
  {
    @Override
    public void process(Serializable tuple)
    {
      sendMessage(tuple);
    }

  };

  /**
   * This is an input port which receives string tuples to be written out to an JMS message bus.
   */
  @InputPortFieldAnnotation(optional = true)
  public final transient DefaultInputPort<String> inputStringTypePort = new DefaultInputPort<String>()
  {
    @Override
    public void process(String tuple)
    {
      sendMessage(tuple);
    }

  };

  /**
   * Create a JMS Message for the given tuple.
   * @return Message
   */
  @Override
  protected Message createMessage(Object tuple)
  {
    try {
      if (tuple instanceof Message) {
        return (Message)tuple;
      } else if (tuple instanceof String) {
        return getSession().createTextMessage((String)tuple);
      } else if (tuple instanceof byte[]) {
        BytesMessage message = getSession().createBytesMessage();
        message.writeBytes((byte[])tuple);
        return message;
      } else if (tuple instanceof Map) {
        return createMessageForMap((Map)tuple);
      } else if (tuple instanceof Serializable) {
        return getSession().createObjectMessage((Serializable)tuple);
      } else {
        throw new RuntimeException("Cannot convert object of type "
            + tuple.getClass() + "] to JMS message. Supported message "
            + "payloads are: String, byte array, Map<String,?>, Serializable object.");
      }
    } catch (JMSException ex) {
      logger.error(ex.getLocalizedMessage());
      throw new RuntimeException(ex);
    }
  }

  /**
   * Create a JMS MapMessage for the given Map.
   *
   * @param map the Map to convert
   * @return the resulting message
   * @throws JMSException if thrown by JMS methods
   */
  private Message createMessageForMap(Map<?,?> map) throws JMSException
  {
    MapMessage message = getSession().createMapMessage();
    for (Map.Entry<?,?> entry: map.entrySet()) {
      if (!(entry.getKey() instanceof String)) {
        throw new RuntimeException("Cannot convert non-String key of type ["
                + entry.getKey().getClass() + "] to JMS MapMessage entry");
      }
      message.setObject((String)entry.getKey(), entry.getValue());
    }
    return message;
  }

  private static final Logger logger = LoggerFactory.getLogger(JMSMultiPortOutputOperator.class);

}
