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
import java.util.Enumeration;
import java.util.HashMap;
import java.util.Map;

import javax.jms.BytesMessage;
import javax.jms.JMSException;
import javax.jms.MapMessage;
import javax.jms.Message;
import javax.jms.ObjectMessage;
import javax.jms.StreamMessage;
import javax.jms.TextMessage;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datatorrent.api.DefaultOutputPort;

/**
 * An implementation of AbstractJMSInputOperator which emits TextMessage,StreamMessage,BytesMessage,MapMessage
 * and ObjectMessage on their respective ports.
 *
 * @since 2.1.0
 */
@org.apache.hadoop.classification.InterfaceStability.Evolving
public class JMSObjectInputOperator extends AbstractJMSInputOperator<Object>
{
  public final transient DefaultOutputPort<String> outputString = new DefaultOutputPort<String>();
  public final transient DefaultOutputPort<byte[]> outputBytes = new DefaultOutputPort<byte[]>();
  public final transient DefaultOutputPort<Map<String, Object>> outputMap = new DefaultOutputPort<Map<String, Object>>();

  /**
   * This implementation converts a TextMessage back to a String, a
   * ByteMessage back to a byte array, a MapMessage back to a Map,
   * and an ObjectMessage back to a Serializable object. Returns
   * the plain Message object in case of an unknown message type.
   *
   * @return payload
   * @throws javax.jms.JMSException
   */
  @Override
  public Object convert(Message message) throws JMSException
  {
    if (message instanceof TextMessage) {
      return ((TextMessage)message).getText();
    } else if (message instanceof StreamMessage) {
      return ((StreamMessage)message).readString();
    } else if (message instanceof BytesMessage) {
      return extractByteArrayFromMessage((BytesMessage)message);
    } else if (message instanceof MapMessage) {
      return extractMapFromMessage((MapMessage)message);
    } else if (message instanceof ObjectMessage) {
      return extractSerializableFromMessage((ObjectMessage)message);
    } else {
      return message;
    }
  }

  /**
   * Extract a byte array from the given {@link BytesMessage}.
   *
   * @param message the message to convert
   * @return the resulting byte array
   * @throws JMSException if thrown by JMS methods
   */
  protected byte[] extractByteArrayFromMessage(BytesMessage message) throws JMSException
  {
    byte[] bytes = new byte[(int)message.getBodyLength()];
    message.readBytes(bytes);
    return bytes;
  }

  /**
   * Extract a Map from the given {@link MapMessage}.
   *
   * @param message the message to convert
   * @return the resulting Map
   * @throws JMSException if thrown by JMS methods
   */
  protected Map<String, Object> extractMapFromMessage(MapMessage message) throws JMSException
  {
    Map<String, Object> map = new HashMap<String, Object>();
    Enumeration<?> en = message.getMapNames();
    while (en.hasMoreElements()) {
      String key = (String)en.nextElement();
      map.put(key, message.getObject(key));
    }
    return map;
  }

  /**
   * Extract a Serializable object from the given {@link ObjectMessage}.
   *
   * @param message the message to convert
   * @return the resulting Serializable object
   * @throws JMSException if thrown by JMS methods
   */
  protected Serializable extractSerializableFromMessage(ObjectMessage message) throws JMSException
  {
    return message.getObject();
  }

  @Override
  @SuppressWarnings("unchecked")
  protected void emit(Object payload)
  {
    if (outputString.isConnected()) {
      outputString.emit((String)payload);
    } else if (outputMap.isConnected()) {
      outputMap.emit((Map<String, Object>)payload);
    } else if (outputBytes.isConnected()) {
      outputBytes.emit((byte[])payload);
    } else {
      output.emit(payload);
    }
  }

  @SuppressWarnings("unused")
  private static final transient Logger logger = LoggerFactory.getLogger(JMSObjectInputOperator.class);

}
