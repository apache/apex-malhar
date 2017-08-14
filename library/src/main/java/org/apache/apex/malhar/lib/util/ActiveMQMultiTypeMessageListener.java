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
package org.apache.apex.malhar.lib.util;

import java.util.Enumeration;
import java.util.HashMap;
import java.util.Map;

import javax.jms.BytesMessage;
import javax.jms.JMSException;
import javax.jms.MapMessage;
import javax.jms.Message;
import javax.jms.ObjectMessage;
import javax.jms.TextMessage;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @since 2.1.0
 */
public class ActiveMQMultiTypeMessageListener extends ActiveMQMessageListener
{
  @Override
  public void onMessage(Message message)
  {
    super.onMessage(message);
    if (message instanceof TextMessage) {
      TextMessage txtMsg = (TextMessage)message;
      String msg = null;
      try {
        msg = txtMsg.getText();
        receivedData.put(countMessages, msg);
      } catch (JMSException ex) {
        logger.debug(ex.getLocalizedMessage());
      }

      logger.debug("Received a TextMessage: {}", msg);
    } else if (message instanceof MapMessage) {
      MapMessage mapMsg = (MapMessage)message;
      Map map = new HashMap();
      try {
        Enumeration en = mapMsg.getMapNames();
        while (en.hasMoreElements()) {
          String key = (String)en.nextElement();
          map.put(key, mapMsg.getObject(key));
        }
        receivedData.put(countMessages, map);

      } catch (JMSException ex) {
        logger.debug(ex.getLocalizedMessage());
      }
      logger.debug("Received a MapMessage: {}", map);
    } else if (message instanceof BytesMessage) {
      BytesMessage bytesMsg = (BytesMessage)message;
      try {
        byte[] byteArr = new byte[(int)bytesMsg.getBodyLength()];
        bytesMsg.readBytes(byteArr);
        receivedData.put(countMessages, byteArr);
      } catch (JMSException ex) {
        logger.debug(ex.getLocalizedMessage());
      }
      logger.debug("Received a ByteMessage: {}", bytesMsg);

    } else if (message instanceof ObjectMessage) {
      ObjectMessage objMsg = (ObjectMessage)message;
      Object msg = null;
      try {
        msg = objMsg.getObject();
        receivedData.put(countMessages, msg);
      } catch (JMSException ex) {
        logger.debug(ex.getLocalizedMessage());
      }
      logger.debug("Received an ObjectMessage: {}", msg);
    } else {
      throw new IllegalArgumentException("Unhandled message type " + message.getClass().getName());
    }
  }

  private static final Logger logger = LoggerFactory.getLogger(ActiveMQMultiTypeMessageListener.class);

}
