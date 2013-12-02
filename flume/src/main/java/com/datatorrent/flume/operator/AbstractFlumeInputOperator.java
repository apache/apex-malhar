/*
 *  Copyright (c) 2012-2013 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.datatorrent.flume.operator;

import java.io.IOException;
import java.io.Serializable;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.concurrent.ArrayBlockingQueue;

import javax.validation.constraints.NotNull;

import com.datatorrent.api.*;
import com.datatorrent.api.Context.OperatorContext;

import com.datatorrent.flume.sink.Server;
import com.datatorrent.flume.sink.Server.Command;
import com.datatorrent.netlet.AbstractLengthPrependerClient;
import com.datatorrent.netlet.DefaultEventLoop;

/**
 *
 * @param <T> Type of the output payload.
 * @author Chetan Narsude <chetan@datatorrent.com>
 */
public abstract class AbstractFlumeInputOperator<T>
        implements InputOperator, ActivationListener<OperatorContext>, IdleTimeHandler, CheckpointListener
{
  public transient DefaultOutputPort<T> output = new DefaultOutputPort<T>();

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

  private transient volatile boolean connected;
  private transient AbstractLengthPrependerClient client = new AbstractLengthPrependerClient()
  {
    @Override
    public void onMessage(byte[] buffer, int offset, int size)
    {
      /* this are all the payload messages */
      Payload payload = new Payload(convert(buffer, offset + 8, size - 8), Server.readLong(buffer, 0));
      try {
        handoverBuffer.put(payload);
      }
      catch (InterruptedException ex) {
        handleException(ex, eventloop);
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
      array[0] = Command.SEEK.getOrdinal();
      Server.writeLong(array, 1, address);
      write(array);

      connected = true;
    }

    @Override
    public void disconnected()
    {
      connected = false;
      super.disconnected();
    }

  };
  private transient ArrayBlockingQueue<Payload> handoverBuffer = new ArrayBlockingQueue<Payload>(1024 * 5);
  private transient int idleCounter;
  private transient int eventCounter;
  private transient DefaultEventLoop eventloop;
  private transient RecoveryAddress recoveryAddress;
  @NotNull
  private String connectAddress;
  private ArrayList<RecoveryAddress> recoveryAddresses = new ArrayList<RecoveryAddress>();

  @Override
  public void setup(OperatorContext context)
  {
    try {
      eventloop = new DefaultEventLoop("EventLoop-" + context.getId());
    }
    catch (IOException ex) {
      throw new RuntimeException(ex);
    }
  }

  @Override
  public void activate(OperatorContext ctx)
  {
    eventloop.start();
    String[] parts = connectAddress.split(":");
    eventloop.connect(new InetSocketAddress(parts[0], Integer.parseInt(parts[1])), client);
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
    for (int i = handoverBuffer.size(); i-- > 0;) {
      Payload payload = handoverBuffer.poll();
      output.emit(payload.payload);
      recoveryAddress.address = payload.location;
      eventCounter++;
    }
  }

  @Override
  public void endWindow()
  {
    if (connected) {
      byte[] array = new byte[9];

      array[0] = Command.WINDOWED.getOrdinal();
      Server.writeInt(array, 1, eventCounter);
      Server.writeInt(array, 5, idleCounter);

      client.write(array);
    }

    recoveryAddresses.add(recoveryAddress);
  }

  @Override
  public void deactivate()
  {
    eventloop.disconnect(client);
    eventloop.stop();
  }

  @Override
  public void teardown()
  {
    eventloop = null;
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
  public String getConnectAddress()
  {
    return connectAddress;
  }

  /**
   * @param connectAddress the connectAddress to set
   */
  public void setConnectAddress(String connectAddress)
  {
    this.connectAddress = connectAddress;
  }

  private static class RecoveryAddress implements Serializable
  {
    long windowId;
    long address;
    private static final long serialVersionUID = 201312021432L;
  }

  @Override
  public void checkpointed(long windowId)
  {
    /* dont do anything */
  }

  @Override
  public void committed(long windowId)
  {
    if (!connected) {
      return;
    }

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

        array[0] = Command.COMMITTED.getOrdinal();

        final long recoveryOffset = ra.address;
        array[1] = (byte)recoveryOffset;
        array[2] = (byte)(recoveryOffset >> 8);
        array[3] = (byte)(recoveryOffset >> 16);
        array[4] = (byte)(recoveryOffset >> 24);
        array[5] = (byte)(recoveryOffset >> 32);
        array[6] = (byte)(recoveryOffset >> 40);
        array[7] = (byte)(recoveryOffset >> 48);
        array[8] = (byte)(recoveryOffset >> 56);

        client.write(array);
      }
      else {
        break;
      }
    }
  }

}
