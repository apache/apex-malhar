/*
 *  Copyright (c) 2012-2013 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.datatorrent.flume.sink;

import java.io.IOException;

import org.apache.flume.Channel;
import org.apache.flume.EventDeliveryException;
import org.apache.flume.Sink;
import org.apache.flume.lifecycle.LifecycleState;

import com.datatorrent.netlet.DefaultEventLoop;
import com.datatorrent.netlet.Listener.ServerListener;

/**
 *
 * @author Chetan Narsude <chetan@datatorrent.com>
 */
public class DTFlumeSink implements Sink
{
  private Channel channel;
  private String name;
  private LifecycleState state;
  private DefaultEventLoop eventloop;
  ServerListener server;

  public DTFlumeSink()
  {
    this.state = LifecycleState.ERROR;
    try {
      server = new Server();
    }
    catch (Exception ex) {
      throw new RuntimeException(ex);
    }
  }

  /* Begin implementing Flume Sink interface */
  @Override
  public void setChannel(Channel chnl)
  {
    channel = chnl;
  }

  @Override
  public Channel getChannel()
  {
    return channel;
  }

  @Override
  public Status process() throws EventDeliveryException
  {
    state = LifecycleState.IDLE;
    return Status.READY;
  }

  @Override
  public void start()
  {
    try {
      eventloop = new DefaultEventLoop("EventLoop-" + getName());
    }
    catch (IOException ex) {
      throw new RuntimeException(ex);
    }
    eventloop.start();
    eventloop.start("localhost", 5033, server);
    state = LifecycleState.START;
  }

  @Override
  public void stop()
  {
    try {
      state = LifecycleState.STOP;
    }
    finally {
      eventloop.stop(server);
      eventloop.stop();
    }
  }

  @Override
  public LifecycleState getLifecycleState()
  {
    return state;
  }

  @Override
  public void setName(String string)
  {
    name = string;
  }

  @Override
  public String getName()
  {
    return name;
  }

  /* End implementing Flume Sink interface */
}
