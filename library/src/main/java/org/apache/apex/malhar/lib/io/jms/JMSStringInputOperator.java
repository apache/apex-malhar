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

import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.StreamMessage;
import javax.jms.TextMessage;

/**
 * A {@link AbstractJMSInputOperator} which emits Strings.
 *
 * @displayName JMS Input (String)
 * @category Messaging
 * @tags jms, input operator, string
 * @since 0.3.3
 */
@org.apache.hadoop.classification.InterfaceStability.Evolving
public class JMSStringInputOperator extends AbstractJMSInputOperator<String>
{
  @Override
  public String convert(Message message) throws JMSException
  {
    if (message instanceof TextMessage) {
      return ((TextMessage)message).getText();
    } else if (message instanceof StreamMessage) {
      return ((StreamMessage)message).readString();
    } else {
      throw new IllegalArgumentException("Unhandled message type " + message.getClass().getName());
    }
  }

  @Override
  protected void emit(String payload)
  {
    output.emit(payload);
  }
}
