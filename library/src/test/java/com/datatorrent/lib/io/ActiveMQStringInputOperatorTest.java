package com.datatorrent.lib.io;

import com.datatorrent.lib.testbench.CollectorTestSink;
import javax.jms.connection;
import javax.jms.deliverymode;
import javax.jms.destination;
import javax.jms.jmsexception;
import javax.jms.messageproducer;
import javax.jms.session;
import javax.jms.textmessage;

import org.apache.activemq.activemqconnectionfactory;
import org.junit.after;
import org.junit.before;
import org.junit.test;

public class activemqstringinputoperatortest implements runnable
{
	private activemqstringinputoperator consumer;
	private boolean terminate = false;
	private session session;
	private connection connection;
	private messageproducer producer;
	private collectortestsink sink = new collectortestsink();
	private thread generator;
	private boolean timerexpired = false;

	private class timer implements runnable
	{
		@override
		public void run()
		{
			try {
				thread.sleep(2000);
			} catch (interruptedexception e) {
			}
			timerexpired = true;
		}
	}
  
  @before
  public void beforetest() throws exception
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
  	if (terminate) {
  		assertTrue("Test failed in execution", false);
  	}
  	Thread timer = new Thread(new Timer());
  	timer.start();
  	consumer.setup(null);
  	while(!timerExpired || (sink.collectedTuples.size() < 100)) {
  	  consumer.emitTuples();
  	  Thread.sleep(10);
  	}
  	consumer.teardown();
  	generator.join();
  	System.out.println("Total Messages collected in sink : " + sink.collectedTuples.size());
  	
  }

	@Override
	public void run()
	{
    	for (int i = 0; i < 100; i++) {
        String myMsg = "My TestMessage " + i;
        TextMessage message;
				try {

					message = session.createTextMessage(myMsg);
	        producer.send(message);
	        System.out.println(message.toString());
				} catch (JMSException e) {
					e.printStackTrace();
				}
    	}
	}
}
