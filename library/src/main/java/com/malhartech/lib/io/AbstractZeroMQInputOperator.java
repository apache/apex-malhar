/*
 *  Copyright (c) 2012 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.malhartech.lib.io;

import com.malhartech.annotation.OutputPortFieldAnnotation;
import com.malhartech.api.*;
import java.util.HashMap;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.zeromq.ZMQ;

/**
 *
 * @author Zhongjian Wang <zhongjian@malhar-inc.com>
 */
public abstract class AbstractZeroMQInputOperator extends BaseOperator implements SyncInputOperator, Runnable
{
  private static final Logger logger = LoggerFactory.getLogger(AbstractZeroMQInputOperator.class);
  private boolean transacted;
  private int maxiumMessages;
  private int receiveTimeOut;
  private String url;
  private String filter;
  protected ZMQ.Context context;
  protected ZMQ.Socket subscriber;
  protected Thread thr =null;
  volatile boolean running = false;

  @OutputPortFieldAnnotation(name="outputPort")
  final public transient DefaultOutputPort<String> outputPort = new DefaultOutputPort<String>(this);

//  public final transient DefaultInputPort<String> inputPort = new DefaultInputPort<String>(this)
//  {
//    @Override
//    public void process(String str)
//    {
//      addTuple(tuple, numerators);
//    }
//  };

  @Override
  public void setup(OperatorConfiguration config)
  {
    super.setup(config);
    context = ZMQ.context(1);
    subscriber = context.socket(ZMQ.SUB);
    url = config.get("url");
    subscriber.connect(url);

    filter = config.get("filter");
    subscriber.subscribe(filter.getBytes());

    maxiumMessages = config.getInt("maximumMessages", 0);
    receiveTimeOut = config.getInt("receiveTimeOut", 0);
    transacted = config.getBoolean("transacted", false);
//    thr = new Thread(this);
//    thr.start();
  }

//  @Override
//  public void process(Object payload)
//  {
////      byte[] message = subscriber.recv(0);
////      String string = new String(message).trim();
//  }
  public abstract void emitMessage(byte[] message);

  @Override
  public Runnable getDataPoller() {
    return this;
  }

  @Override
  public void run() {
    running = true;
    while(running) {
      try{
          byte[] message = subscriber.recv(0);
          if( message!= null )
            onMessage(message);
      }
      catch(Exception e){
        logger.debug(e.toString());
      }
    }
  }

  public void onMessage(byte[] message) {
    emitMessage(message);
  }
  @Override
  public void teardown()
  {
      running = false;
      subscriber.close();
      context.term();
  }

}
