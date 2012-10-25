/*
 *  Copyright (c) 2012 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.malhartech.contrib.zmq;

import javax.validation.constraints.NotNull;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.zeromq.ZMQ;

import com.malhartech.annotation.OutputPortFieldAnnotation;
import com.malhartech.api.BaseOperator;
import com.malhartech.api.DefaultOutputPort;
import com.malhartech.api.OperatorConfiguration;
import com.malhartech.api.SyncInputOperator;

/**
 *
 * @author Zhongjian Wang <zhongjian@malhar-inc.com>
 */
public abstract class AbstractZeroMQInputOperator<T> extends BaseOperator implements SyncInputOperator, Runnable
{
  private static final Logger logger = LoggerFactory.getLogger(AbstractZeroMQInputOperator.class);
  private String url;
  private String syncUrl;
  private String filter;
  protected ZMQ.Context context;
  protected ZMQ.Socket subscriber;
  protected ZMQ.Socket syncclient;
  volatile boolean running = false;

  @OutputPortFieldAnnotation(name="outputPort")
  final public transient DefaultOutputPort<T> outputPort = new DefaultOutputPort<T>(this);

  @NotNull
  public void setUrl(String url) {
    this.url = url;
  }
  @NotNull
  public void setSyncUrl(String url) {
    this.syncUrl = url;
  }
  @NotNull
  public void setFilter(String filter) {
    this.filter = filter;
  }

  @Override
  public void setup(OperatorConfiguration config)
  {
    super.setup(config);
    context = ZMQ.context(1);
    subscriber = context.socket(ZMQ.SUB);
    subscriber.connect(url);
    subscriber.subscribe(filter.getBytes());
    syncclient = context.socket(ZMQ.REQ);
    syncclient.connect(syncUrl);
    syncclient.send("".getBytes(), 0);
  }

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
          if( message!= null ) {
            onMessage(message);
          }
      }
      catch(Exception e){
//        logger.debug(e.toString());
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
      syncclient.close();
      context.term();
  }

}
