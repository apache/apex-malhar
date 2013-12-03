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
    private byte ord;

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
      assert (size > 0);

      Request r;
      switch (Command.getCommand(buffer[offset])) {
        case COMMITTED:
          r = new Request(Command.COMMITTED, readLong(buffer, offset + 1));
          break;

        case CHECKPOINTED:
          r = null;
          break;

        case SEEK:
          r = new Request(Command.SEEK, readLong(buffer, offset + 1));
          break;

        case WINDOWED:
          r = new Request(Command.WINDOWED, readLong(buffer, offset + 1));
          break;

        case ECHO:
          write(buffer, offset, size);
          return;

        default:
          r = null;
      }

      synchronized (requests) {
        requests.add(r);
      }
    }

    @Override
    public void connected()
    {
      super.connected();
      Server.this.client = this;

      logger.debug("some client connected!");
      synchronized (requests) {
        requests.add(new Request(Command.CONNECTED, 0));
      }
    }

    @Override
    public void disconnected()
    {
      synchronized (requests) {
        requests.add(new Request(Command.DISCONNECTED, 0));
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

  public class Request
  {
    public final Command type;
    public final long address;

    Request(Command type, long address)
    {
      this.type = type;
      this.address = address;
    }

    @Override
    public String toString()
    {
      return "Request{" + "type=" + type + ", address=" + address + '}';
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
