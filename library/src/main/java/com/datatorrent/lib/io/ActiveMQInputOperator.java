/*
 * Copyright (c) 2013 Malhar Inc. ALL Rights Reserved.
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
package com.datatorrent.lib.io;

import java.util.ArrayList;

import javax.jms.ExceptionListener;
import javax.jms.JMSException;

import com.datatorrent.api.Context.OperatorContext;
import com.datatorrent.api.annotation.OutputPortFieldAnnotation;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.api.InputOperator;

import org.apache.activemq.ActiveMQConnectionFactory;

import javax.jms.Connection;
import javax.jms.Destination;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.Session;

/**
 * <p>Abstract ActiveMQInputOperator class.</p>
 *
 * @since 0.3.3
 */
public abstract class ActiveMQInputOperator<T> implements  ExceptionListener, InputOperator, Runnable
{
  /**
   * User/password fields.
   */
	private String user = "";
	private String password = "";
	private String url = "vm://localhost:61617";
	private String queueName = "DATATORRENT.QUEUE";
	private boolean terminate = false;
	private boolean transacted = false;
	private int ackMode = Session.AUTO_ACKNOWLEDGE;

	// Active MQ connection fields 
	private Connection connection;
	private Session session;
	private MessageConsumer consumer;
	private Destination destination;
	private Thread consumerThread;
	private boolean hasException = false;
	private Object lock = new Object();
	private ArrayList<Message> messages = new ArrayList<Message>();
	
	/**
	 * Output port
	 */
	@OutputPortFieldAnnotation(name = "outport")
	public final transient DefaultOutputPort<T> outport = new DefaultOutputPort<T>();
	
	@Override
	public void beginWindow(long windowId)
	{
	}

	@Override
	public void endWindow()
	{
	}

	@Override
	public void setup(OperatorContext context)
	{
		ActiveMQConnectionFactory connectionFactory = new ActiveMQConnectionFactory(url);
  	try {
			connection = connectionFactory.createConnection();
    	connection.start();
  		connection.setExceptionListener(this);
      session = connection.createSession(transacted, ackMode);
      destination = session.createQueue(queueName);
      consumer = session.createConsumer(destination);
		} catch (JMSException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
    consumerThread = new Thread(this);
    consumerThread.start();
	}

	@Override
	public void teardown()
	{
    try {
    	terminate = true;
			consumerThread.join();
			connection.close();
			session.close();
			consumer.close();
		} catch (JMSException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
	  } catch (InterruptedException e) {
				e.printStackTrace();
		}
	}

	@Override
	public void emitTuples()
	{
		if (hasException) return;
		synchronized (lock) {
			for (int i=0; i < messages.size(); i++) {
				outport.emit(convertActiveMessage(messages.get(i)));
				//System.out.println("Emit Message" + messages.get(i).toString());
			}
			messages = new ArrayList<Message>();
		}
	}

	@Override
	public void onException(JMSException exception)
	{
		hasException = true;
	}

	@Override
	public void run()
	{
    while (!terminate) {
    	try {
        Message message = consumer.receiveNoWait();
        if (message == null) continue;
        synchronized(lock) {
        	messages.add(message);
        }
       /* if (message != null) {
						System.out.println("Received Message" + message.toString());
        }*/
			} catch (JMSException e) {
				e.printStackTrace();
				System.out.println(e.toString());
				hasException = true;
				break;
			} 
    }
	}

	public String getUser()
	{
		return user;
	}

	public void setUser(String user)
	{
		this.user = user;
	}

	public String getPassword()
	{
		return password;
	}

	public void setPassword(String password)
	{
		this.password = password;
	}

	public String getUrl()
	{
		return url;
	}

	public void setUrl(String url)
	{
		this.url = url;
	}

	public String getQueueName()
	{
		return queueName;
	}

	public void setQueueName(String queueName)
	{
		this.queueName = queueName;
	}
	
	// convert message to given type
	public abstract T convertActiveMessage(Message message);

	public boolean isTransacted()
	{
		return transacted;
	}

	public void setTransacted(boolean transacted)
	{
		this.transacted = transacted;
	}

	public int getAckMode()
	{
		return ackMode;
	}

	public void setAckMode(int ackMode)
	{
		this.ackMode = ackMode;
	}
}
