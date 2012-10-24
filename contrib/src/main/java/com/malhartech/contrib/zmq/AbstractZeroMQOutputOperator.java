/*
 *  Copyright (c) 2012 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.malhartech.contrib.zmq;

import com.malhartech.api.BaseOperator;
import com.malhartech.api.DefaultInputPort;
import com.malhartech.api.OperatorConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.zeromq.ZMQ;

/**
 *
 * @author Zhongjian Wang <zhongjian@malhar-inc.com>
 */
public class AbstractZeroMQOutputOperator<T> extends BaseOperator
{
  private static final Logger logger = LoggerFactory.getLogger(AbstractZeroMQInputOperator.class);
  private boolean transacted;
  private int maxiumMessages;
  private int receiveTimeOut;
  private ZMQ.Context context;
  private ZMQ.Socket publisher;
  private String addr;

  @Override
  public void setup(OperatorConfiguration config)
  {
    super.setup(config);
    //setupConnection(config);
    context = ZMQ.context(1);
    logger.debug("Publishing on ZeroMQ");
    publisher = context.socket(ZMQ.PUB);
    addr = config.get("url");
    publisher.bind(addr);

    maxiumMessages = config.getInt("maximumMessages", 0);
    receiveTimeOut = config.getInt("receiveTimeOut", 0);
    transacted = config.getBoolean("transacted", false);
  }

  public final transient DefaultInputPort<Object> input = new DefaultInputPort<Object>(this)
  {
    @Override
    public void process(Object payload)
    {
      String msg = payload.toString();
      publisher.send(msg.getBytes(), 0);
    }
  };

/*  //@Override
  public void process(Object payload)
  {
    // convert payload to message
    String msg = payload.toString();
    publisher.send(msg.getBytes(), 0);
  }*/
  /*
  @Override
  public void process(Object payload)
  {
    while(true) {
      String data = "abc";
      logger.debug("publishing data:"+data);
      publisher.send(data.getBytes(), 0);
      try {
        Thread.sleep(1000);
      }
      catch (InterruptedException ex) {
        java.util.logging.Logger.getLogger(AbstractZeroMQOutputOperator.class.getName()).log(Level.SEVERE, null, ex);
      }
    }
    //throw new UnsupportedOperationException("Not supported yet.");
  }
*/
  @Override
  public void teardown()
  {
      publisher.close();
      context.term();
  }
}
