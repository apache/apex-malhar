/*
 *  Copyright (c) 2012 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.malhartech.contrib.zmq;

import com.malhartech.annotation.InjectConfig;
import javax.validation.constraints.NotNull;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.zeromq.ZMQ;

import com.malhartech.api.ActivationListener;
import com.malhartech.api.InputOperator;
import com.malhartech.api.BaseOperator;
import com.malhartech.api.Context.OperatorContext;
import com.malhartech.util.CircularBuffer;
import javax.validation.constraints.Min;

/**
 *
 * @author Zhongjian Wang <zhongjian@malhar-inc.com>
 */
public abstract class AbstractBaseZeroMQInputOperator extends BaseOperator implements InputOperator, ActivationListener<OperatorContext>
{
  private static final Logger logger = LoggerFactory.getLogger(AbstractBaseZeroMQInputOperator.class);
  protected ZMQ.Context context;
  protected ZMQ.Socket subscriber;
  protected ZMQ.Socket syncclient;
  @InjectConfig(key = "url")
  private String url;
  @InjectConfig(key = "syncUrl")
  private String syncUrl;
  @InjectConfig(key = "filter")
  private String filter;

  @InjectConfig(key = "tuple_blast")
  private int tuple_blast = 1000;
  private volatile boolean running = false;
  CircularBuffer<byte[]> tempBuffer = new CircularBuffer<byte[]>(1024 * 1024);

  @NotNull
  public void setUrl(String url)
  {
    this.url = url;
  }

  @NotNull
  public void setSyncUrl(String url)
  {
    this.syncUrl = url;
  }

  @NotNull
  public void setFilter(String filter)
  {
    this.filter = filter;
  }

  @Min(1)
  public void setTupleBlast(int i)
  {
    this.tuple_blast = i;
  }
  @Override
  public void setup(OperatorContext ctx)
  {
    context = ZMQ.context(1);
    subscriber = context.socket(ZMQ.SUB);
    subscriber.connect(url);
    subscriber.subscribe(filter.getBytes());
    syncclient = context.socket(ZMQ.REQ);
    syncclient.connect(syncUrl);
    syncclient.send("".getBytes(), 0);
  }

  @Override
  public void teardown()
  {
    subscriber.close();
    syncclient.close();
    context.term();
  }

  // The other thread
  public void activate(OperatorContext ctx)
  {
    new Thread()
    {
      @Override
      public void run()
      {
        running = true;
        while (running) {
          try {
            byte[] message = subscriber.recv(0);
            if (message != null) {
              tempBuffer.add(message);
            }
          }
          catch (Exception e) {
//        logger.debug(e.toString());
            break;
          }
        }
      }
    }.start();
  }

  public void deactivate()
  {
    running = false;
  }
}