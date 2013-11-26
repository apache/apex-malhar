/*
 *  Copyright (c) 2012-2013 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.datatorrent.flume.sink;

import com.datatorrent.netlet.DefaultEventLoop;
import com.datatorrent.netlet.Listener.ServerListener;
import org.apache.flume.Channel;
import org.apache.flume.EventDeliveryException;
import org.apache.flume.Sink;
import org.apache.flume.lifecycle.LifecycleState;

/**
 *
 * @author Chetan Narsude <chetan@datatorrent.com>
 */
public class DTFlumeSink implements Sink
{

  /**
   * @return the eventloop
   */
  public DefaultEventLoop getEventloop()
  {
    return eventloop;
  }

  /**
   * @param eventloop the eventloop to set
   */
  public void setEventloop(DefaultEventLoop eventloop)
  {
    this.eventloop = eventloop;
  }
  public enum MESSAGES
  {
    OK,
    BEGIN,
    COMMIT,
    ROLLBACK
  }

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
    getEventloop().start("localhost", 5033, server);
    state = LifecycleState.START;
  }

  @Override
  public void stop()
  {
    try {
      state = LifecycleState.STOP;
    }
    finally {
      getEventloop().stop(server);
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

}
