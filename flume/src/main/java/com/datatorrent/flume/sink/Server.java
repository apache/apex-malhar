/*
 *  Copyright (c) 2012-2013 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.datatorrent.flume.sink;

import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.util.ArrayList;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datatorrent.netlet.AbstractLengthPrependerClient;

/**
 *
 * @author Chetan Narsude <chetan@datatorrent.com>
 */
public class Server extends com.datatorrent.netlet.Server
{
  public enum Command
  {
    ECHO((byte)0),
    COMMITTED((byte)1),
    CHECKPOINTED((byte)2),
    SEEK((byte)3),
    CONNECTED((byte)4),
    DISCONNECTED((byte)5),
    WINDOWED((byte)6);

    Command(byte b)
    {
      this.ord = b;
    }

    public byte getOrdinal()
    {
      return ord;
    }

    public static Command getCommand(byte b)
    {
      switch (b) {
        case 0:
          return ECHO;

        case 1:
          return COMMITTED;

        case 2:
          return CHECKPOINTED;

        case 3:
          return SEEK;

        case 4:
          return CONNECTED;

        case 5:
          return DISCONNECTED;

        case 6:
          return WINDOWED;

        default:
          return null;
      }
    }

    private byte ord;
  }

  Client client;
  public final ArrayList<Request> requests = new ArrayList<Request>(4);

  @Override
  public ClientListener getClientConnection(SocketChannel sc, ServerSocketChannel ssc)
  {
    Client lClient = new Client();
    lClient.connected();
    return lClient;
  }

  public class Client extends AbstractLengthPrependerClient
  {
    int idleCount;

    @Override
    public void onMessage(byte[] buffer, int offset, int size)
    {
      if (Command.getCommand(buffer[offset]) == Command.ECHO) {
        write(buffer, offset, size);
        return;
      }

      Request r = Request.getRequest(buffer, offset);
      synchronized (requests) {
        requests.add(r);
      }
    }

    @Override
    public void connected()
    {
      super.connected();
      Server.this.client = this;

      synchronized (requests) {
        requests.add(Request.getRequest(new byte[] {Command.CONNECTED.getOrdinal(), 0, 0, 0, 0, 0, 0, 0, 0}, 0));
      }
    }

    @Override
    public void disconnected()
    {
      synchronized (requests) {
        requests.add(Request.getRequest(new byte[] {Command.DISCONNECTED.getOrdinal(), 0, 0, 0, 0, 0, 0, 0, 0}, 0));
      }
      Server.this.client = null;
      super.disconnected();
    }

    public void write(long l, byte[] bytes)
    {
      byte[] array = new byte[bytes.length + 8];
      writeLong(array, 0, l);
      System.arraycopy(bytes, 0, array, 8, bytes.length);
      write(array);
    }

  }

  public static abstract class Request
  {
    public final Command type;

    public Request(Command type)
    {
      this.type = type;
    }

    public abstract long getAddress();

    public abstract int getEventCount();

    public abstract int getIdleCount();

    @Override
    public String toString()
    {
      return "Request{" + "type=" + type + '}';
    }

    public static Request getRequest(final byte[] buffer, final int offset)
    {
      Command command = Command.getCommand(buffer[offset]);
      switch (command) {
        case WINDOWED:
          return new Request(Command.WINDOWED)
          {
            final int eventCount;
            final int idleCount;

            {
              eventCount = Server.readInt(buffer, offset + 1);
              idleCount = Server.readInt(buffer, offset + 5);
            }

            @Override
            public long getAddress()
            {
              throw new UnsupportedOperationException();
            }

            @Override
            public int getEventCount()
            {
              return eventCount;
            }

            @Override
            public int getIdleCount()
            {
              return idleCount;
            }

            @Override
            public String toString()
            {
              return "Request{" + "type=" + type + ", eventCount=" + eventCount + ", idleCount=" + idleCount + '}';
            }

          };

        default:
          return new Request(command)
          {
            final long address;

            {
              address = Server.readLong(buffer, offset + 1);
            }

            @Override
            public long getAddress()
            {
              return address;
            }

            @Override
            public int getEventCount()
            {
              throw new UnsupportedOperationException();
            }

            @Override
            public int getIdleCount()
            {
              throw new UnsupportedOperationException();
            }

            @Override
            public String toString()
            {
              return "Request{" + "type=" + type + ", address=" + address + '}';
            }

          };

      }

    }

  }

  public static int readInt(byte[] buffer, int offset)
  {
    return buffer[offset++] & 0xff
           | (buffer[offset++] & 0xff) << 8
           | (buffer[offset++] & 0xff) << 16
           | (buffer[offset++] & 0xff) << 24;
  }

  public static void writeInt(byte[] buffer, int offset, int i)
  {
    buffer[offset++] = (byte)i;
    buffer[offset++] = (byte)(i >>> 8);
    buffer[offset++] = (byte)(i >>> 16);
    buffer[offset++] = (byte)(i >>> 24);
  }

  public static long readLong(byte[] buffer, int offset)
  {
    return (long)buffer[offset++] & 0xff
           | (long)(buffer[offset++] & 0xff) << 8
           | (long)(buffer[offset++] & 0xff) << 16
           | (long)(buffer[offset++] & 0xff) << 24
           | (long)(buffer[offset++] & 0xff) << 32
           | (long)(buffer[offset++] & 0xff) << 40
           | (long)(buffer[offset++] & 0xff) << 48
           | (long)(buffer[offset++] & 0xff) << 56;
  }

  public static void writeLong(byte[] buffer, int offset, long l)
  {
    buffer[offset++] = (byte)l;
    buffer[offset++] = (byte)(l >>> 8);
    buffer[offset++] = (byte)(l >>> 16);
    buffer[offset++] = (byte)(l >>> 24);
    buffer[offset++] = (byte)(l >>> 32);
    buffer[offset++] = (byte)(l >>> 40);
    buffer[offset++] = (byte)(l >>> 48);
    buffer[offset++] = (byte)(l >>> 56);
  }

  private static final Logger logger = LoggerFactory.getLogger(Server.class);
}
