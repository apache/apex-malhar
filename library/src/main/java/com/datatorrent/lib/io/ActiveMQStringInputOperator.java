package com.datatorrent.lib.io;

import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.StreamMessage;
import javax.jms.TextMessage;

public class ActiveMQStringInputOperator extends ActiveMQInputOperator<String>
{
	@Override
	public String convertActiveMessage(Message message)
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
