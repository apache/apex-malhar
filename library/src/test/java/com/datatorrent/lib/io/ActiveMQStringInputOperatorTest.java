package com.datatorrent.lib.io;

import com.datatorrent.lib.testbench.CollectorTestSink;
import javax.jms.Connection;
import javax.jms.DeliveryMode;
import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.MessageProducer;
import javax.jms.Session;
import javax.jms.TextMessage;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class ActiveMQStringInputOperatorTest implements Runnable
{
	private ActiveMQStringInputOperator consumer;
	private boolean terminate = false;
	private Session session;
	private Connection connection;
	private MessageProducer producer;
	private CollectorTestSink sink = new CollectorTestSink();
	private Thread generator;

  @Before
  public void beforeTest() throws Exception
  {

  	// Active MQ consumer operator
  	consumer = new ActiveMQStringInputOperator();
    consumer.outport.setSink(sink);

    // Create a ConnectionFactory
    ActiveMQConnectionFactory connectionFactory = new ActiveMQConnectionFactory("vm://localhost");

    // Create a Connection
    connection = connectionFactory.createConnection();
    connection.start();

    // Create a Session
    session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);

    // Create the destination (Topic or Queue)
    Destination destination = session.createQueue("DATATORRENT.QUEUE");

    // Create a MessageProducer from the Session to the Topic or Queue
    producer = session.createProducer(destination);
    producer.setDeliveryMode(DeliveryMode.NON_PERSISTENT);

    // start producer thread
    generator = new Thread(this);
    generator.start();
  }

  @After
  public void afterTest() throws Exception
  {
  }

  @Test
  public void testActiveMQInputOperator() throws Exception
  {
  	consumer.setup(null);
  	Thread.sleep(2000);
  	consumer.emitTuples();
  	consumer.teardown();
  	terminate = true;
  	generator.join();
  	System.out.println("Total Messages collected in sink : " + sink.collectedTuples.size());
  }

	@Override
	public void run()
	{
		while(!terminate)
		{
    	for (int i = 0; i < 10; i++) {
        String myMsg = "My TestMessage " + i;
        TextMessage message;
				try {

					message = session.createTextMessage(myMsg);
	        producer.send(message);
	        //System.out.println(message.toString());
				} catch (JMSException e) {
					e.printStackTrace();
				}
    	}

    	try {
  			Thread.sleep(100);
  		} catch (InterruptedException e) {
  			// TODO Auto-generated catch block
  			e.printStackTrace();
  		}
   }
	}
}
