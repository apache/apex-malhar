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
import com.datatorrent.api.CheckpointListener;
import com.datatorrent.api.Context.OperatorContext;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.api.IdleTimeHandler;
import com.datatorrent.api.InputOperator;
import com.datatorrent.flume.sink.Server;

import com.datatorrent.netlet.AbstractLengthPrependerClient;
import com.datatorrent.netlet.DefaultEventLoop;
import java.util.Iterator;

/**
 *
 * @param <T> Type of the output payload.
 * @author Chetan Narsude <chetan@datatorrent.com>
 */
public abstract class AbstractFlumeInputOperator<T>
        extends AbstractLengthPrependerClient
        implements InputOperator, ActivationListener<OperatorContext>, IdleTimeHandler, CheckpointListener
{
  public transient DefaultOutputPort<T> output = new DefaultOutputPort<T>();
  private transient ArrayList<Payload> handoverBuffer = new ArrayList<Payload>(1024);
  private transient int idleCounter;
  private transient int eventCounter;
  private transient DefaultEventLoop eventloop;
  @NotNull
  private InetSocketAddress connectAddress;
  ArrayList<RecoveryAddress> recoveryAddresses = new ArrayList<RecoveryAddress>();
  private transient RecoveryAddress recoveryAddress;

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
    recoveryAddress = new RecoveryAddress();
    recoveryAddress.windowId = windowId;
    idleCounter = 0;
    eventCounter = 0;
  }

  @Override
  public synchronized void emitTuples()
  {
    for (Payload payload : handoverBuffer) {
      output.emit(payload.payload);
      recoveryAddress.address = payload.location;
      eventCounter++;
    }
  }

  @Override
  public void endWindow()
  {
    byte[] array = new byte[9];

    array[0] = Server.WINDOWED;
    Server.writeInt(buffer, 1, eventCounter);
    Server.writeInt(buffer, 6, idleCounter);

    write(array);

    recoveryAddresses.add(recoveryAddress);
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
    Payload payload = new Payload(convert(buffer, offset + 8, size - 8), Server.readLong(buffer, 0));
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

  private static class RecoveryAddress
  {
    long windowId;
    long address;
  }

  @Override
  public void checkpointed(long windowId)
  {
    /* dont do anything */
  }

  @Override
  public void committed(long windowId)
  {
    Iterator<RecoveryAddress> iterator = recoveryAddresses.iterator();
    while (iterator.hasNext()) {
      RecoveryAddress ra = iterator.next();
      if (ra.windowId < windowId) {
        iterator.remove();
      }
      else if (ra.windowId == windowId) {
        iterator.remove();
        int arraySize = 1/* for the type of the message */
                        + 8 /* for the location to commit */;
        byte[] array = new byte[arraySize];

        array[0] = Server.COMMITED;

        final long recoveryOffset = ra.address;
        array[1] = (byte)recoveryOffset;
        array[2] = (byte)(recoveryOffset >> 8);
        array[3] = (byte)(recoveryOffset >> 16);
        array[4] = (byte)(recoveryOffset >> 24);
        array[5] = (byte)(recoveryOffset >> 32);
        array[6] = (byte)(recoveryOffset >> 40);
        array[7] = (byte)(recoveryOffset >> 48);
        array[8] = (byte)(recoveryOffset >> 56);

        write(array);
      }
      else {
        break;
      }
    }
  }

  @Override
  public void connected()
  {
    super.connected();

    long address;
    if (recoveryAddresses.size() > 0) {
      address = recoveryAddresses.get(recoveryAddresses.size() - 1).address;
    }
    else {
      address = 0;
    }

    int len = 1 /* for the message type SEEK */
              + 8 /* for the address */;

    byte[] array = new byte[len];
    array[0] = Server.SEEK;
    Server.writeLong(array, 1, address);
    write(array);
  }

}
