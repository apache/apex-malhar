package com.datatorrent.contrib.zmq;

import java.util.HashMap;

import org.zeromq.ZMQ;


final class ZeroMQMessageReceiver implements Runnable
{
  public HashMap<String, Integer> dataMap = new HashMap<String, Integer>();
  public int count = 0;
  protected ZMQ.Context context;
  protected ZMQ.Socket subscriber;
  protected ZMQ.Socket syncclient;
  volatile boolean shutDown = false;
  
  private static org.slf4j.Logger logger;
  
  public ZeroMQMessageReceiver(org.slf4j.Logger loggerInstance)
  {
	logger =  loggerInstance;
  }
  
  public void setup()
  {
    context = ZMQ.context(1);
    logger.debug("Subsribing on ZeroMQ");
    subscriber = context.socket(ZMQ.SUB);
    subscriber.connect("tcp://localhost:5556");
    subscriber.subscribe("".getBytes());
    syncclient = context.socket(ZMQ.REQ);
    syncclient.connect("tcp://localhost:5557");
    sendSync();
  }

  public void sendSync()
  {
    syncclient.send("".getBytes(), 0);
  }

  @Override
  public void run()
  {
    logger.debug("receiver running");      
    while (!Thread.currentThread().isInterrupted() && !shutDown) {
    	//logger.debug("receiver running in loop"); 
      byte[] msg = subscriber.recv(ZMQ.NOBLOCK);
      // convert to HashMap and save the values for each key
      // then expect c to be 1000, b=20, a=2
      // and do count++ (where count now would be 30)
      if(msg == null || msg.length ==0)
      {
    	  continue;
      }
      String str = new String(msg);
      
      if (str.indexOf("{") == -1) {
        continue;
      }
      int eq = str.indexOf('=');
      String key = str.substring(1, eq);
      int value = Integer.parseInt(str.substring(eq + 1, str.length() - 1));
      logger.debug("\nsubscriber recv:" + str);
      dataMap.put(key, value);
      count++;
      logger.debug("out of loop.. ");
    }
  }

  public void teardown()
  {
	shutDown=true;
	
	syncclient.close();
    subscriber.close();
    context.term();
  }
}
