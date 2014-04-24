/*
 *  Copyright (c) 2012-2014 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.datatorrent.apps.etl;

import com.datatorrent.api.BaseOperator;
import com.datatorrent.api.Context.OperatorContext;
import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.api.annotation.InputPortFieldAnnotation;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 *
 * @param <T>
 * @author Ashwin Chandra Putta <ashwin@datatorrent.com>
 */
public abstract class WebSocketServerOperatorBase<T> extends BaseOperator implements WebSocketCallBackBase
{
  EmbeddedWebSocketServer<String, OperatorContext> server;

  @InputPortFieldAnnotation(name = "in", optional = true)
  public final transient DefaultInputPort<T> input = new DefaultInputPort<T>()
  {
    @Override
    public void process(T t)
    {
      processTuple(t);
    }
  };

  protected abstract void processTuple(T t);

  @Override
  public void setup(OperatorContext context)
  {
    try {
      server = new EmbeddedWebSocketServer<String, OperatorContext>();
      server.setup(context);
      server.setCallBackBase(this);
      server.start();
    }
    catch (Exception ex) {
      throw new RuntimeException(ex);
    }
  }

  @Override
  public void teardown()
  {
    try {
      server.stop();
    }
    catch (Exception ex) {
      throw new RuntimeException(ex);
    }
  }
}
