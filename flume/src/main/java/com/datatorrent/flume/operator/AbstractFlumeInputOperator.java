/*
 *  Copyright (c) 2012-2013 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.datatorrent.flume.operator;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.ArrayList;

import javax.validation.constraints.NotNull;

import com.datatorrent.api.ActivationListener;
import com.datatorrent.api.Context.OperatorContext;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.api.IdleTimeHandler;
import com.datatorrent.api.InputOperator;

import com.datatorrent.netlet.AbstractLengthPrependerClient;
import com.datatorrent.netlet.DefaultEventLoop;

/**
 *
 * @param <T> Type of the output payload.
 * @author Chetan Narsude <chetan@datatorrent.com>
 */
public abstract class AbstractFlumeInputOperator<T> extends AbstractLengthPrependerClient
        implements InputOperator, ActivationListener<OperatorContext>, IdleTimeHandler
{
  public transient DefaultOutputPort<T> output = new DefaultOutputPort<T>();
  private transient ArrayList<Payload> handoverBuffer = new ArrayList<Payload>(1024);
  private transient int idleCounter;
  private transient DefaultEventLoop eventloop;
  @NotNull
  private InetSocketAddress connectAddress;
  private long recoveryOffset;

  @Override
  public void setup(OperatorContext context)
  {
    try {
      eventloop = new DefaultEventLoop("EventLoop-" + context.getId());
    }
    catch (IOException ex) {
      throw new RuntimeException();
    }
  }

  @Override
  public void activate(OperatorContext ctx)
  {
    eventloop.start();
    eventloop.connect(connectAddress, this);
  }

  @Override
  public void beginWindow(long windowId)
  {
    int arraySize = 8 /* for the location to commit */
                    + 4 /* for idleTimeout */;
    byte[] array = new byte[arraySize];

    array[0] = (byte)recoveryOffset;
    array[1] = (byte)(recoveryOffset >> 8);
    array[2] = (byte)(recoveryOffset >> 16);
    array[3] = (byte)(recoveryOffset >> 24);
    array[4] = (byte)(recoveryOffset >> 32);
    array[5] = (byte)(recoveryOffset >> 40);
    array[6] = (byte)(recoveryOffset >> 48);
    array[7] = (byte)(recoveryOffset >> 56);

    array[8] = (byte)(idleCounter);
    array[9] = (byte)(idleCounter >> 8);
    array[10] = (byte)(idleCounter >> 16);
    array[11] = (byte)(idleCounter >> 24);

    write(array);
    idleCounter = 0;
  }

  @Override
  public synchronized void emitTuples()
  {
    for (Payload payload : handoverBuffer) {
      output.emit(payload.payload);
      recoveryOffset = payload.location;
    }
  }

  @Override
  public void endWindow()
  {
    /* dont do anything here */
  }

  @Override
  public void deactivate()
  {
    eventloop.disconnect(this);
    eventloop.stop();
  }

  @Override
  public void teardown()
  {
    eventloop = null;
  }

  @Override
  public void onMessage(byte[] buffer, int offset, int size)
  {
    /* this are all the payload messages */
    Payload payload = new Payload(convert(buffer, offset + 8, size - 8),
                                  buffer[offset++]
                                  | buffer[offset++] << 8
                                  | buffer[offset++] << 16
                                  | buffer[offset++] << 24
                                  | buffer[offset++] << 32
                                  | buffer[offset++] << 40
                                  | buffer[offset++] << 48
                                  | buffer[offset++] << 56);
    synchronized (this) {
      handoverBuffer.add(payload);
    }
  }

  @Override
  public void handleIdleTime()
  {
    idleCounter++;
  }

  public abstract T convert(byte[] buffer, int offset, int size);

  /**
   * @return the connectAddress
   */
  public InetSocketAddress getConnectAddress()
  {
    return connectAddress;
  }

  /**
   * @param connectAddress the connectAddress to set
   */
  public void setConnectAddress(InetSocketAddress connectAddress)
  {
    this.connectAddress = connectAddress;
  }

  private class Payload
  {
    final T payload;
    final long location;

    Payload(T payload, long location)
    {
      this.payload = payload;
      this.location = location;
    }

  }

}
