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
import javax.jms.Message;
import javax.jms.StreamMessage;
import javax.jms.TextMessage;

/**
 * This operator ingests strings from an ActiveMQ message bus.&nbsp;
 * This operator has a single output port.
 * <p></p>
 * @displayName Active MQ Single Port Input (String)
 * @category messaging
 * @tags jms, input, string
 *
 * @since 0.3.3
 */
public class ActiveMQSinglePortStringInputOperator extends AbstractActiveMQSinglePortInputOperator<String>
{
	@Override
	public String getTuple(Message message)
	{
		String msg = null;
    try {
      if (message instanceof TextMessage) {
        msg = ((TextMessage)message).getText();
        //logger.debug("Received Message: {}", msg);
      }
      else if (message instanceof StreamMessage) {
        msg = ((StreamMessage)message).readString();
      }
      else {
        throw new IllegalArgumentException("Unhandled message type " + message.getClass().getName());
      }
    }
    catch (JMSException ex) {
      return msg;
    }
    return msg;
	}

}
