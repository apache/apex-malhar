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
import com.datatorrent.storage.Storage;

/**
 *
 * @author Chetan Narsude <chetan@datatorrent.com>
 */
public class Server extends com.datatorrent.netlet.Server
{
  public static final byte COMMITED = 1;
  public static final byte CHECKPOINTED = 2;
  public static final byte SEEK = 3;
  public static final byte CONNECTED = 4;
  public static final byte DISCONNECTED = 5;
  public static final byte WINDOWED = 6;
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
      switch (buffer[offset]) {
        case COMMITED:
          r = new Request(COMMITED, readLong(buffer, offset + 1));
          break;

        case CHECKPOINTED:
          r = null;
          break;

        case SEEK:
          r = new Request(SEEK, readLong(buffer, offset + 1));
          break;

        case WINDOWED:
          r = new Request(WINDOWED, readLong(buffer, offset + 1));
          break;

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
        requests.add(new Request(CONNECTED, 0));
      }
    }

    @Override
    public void disconnected()
    {
      synchronized (requests) {
        requests.add(new Request(DISCONNECTED, 0));
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
    public final byte type;
    public final long address;

    Request(byte type, long address)
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
    return buffer[offset]
           | buffer[offset++] << 8
           | buffer[offset++] << 16
           | buffer[offset++] << 24;
  }

  public static void writeInt(byte[] buffer, int offset, int i)
  {
    buffer[offset++] = (byte)i;
    buffer[offset++] = (byte)(i >> 8);
    buffer[offset++] = (byte)(i >> 16);
    buffer[offset++] = (byte)(i >> 24);
  }

  public static long readLong(byte[] buffer, int offset)
  {
    return buffer[offset]
           | buffer[offset++] << 8
           | buffer[offset++] << 16
           | buffer[offset++] << 24
           | buffer[offset++] << 32
           | buffer[offset++] << 40
           | buffer[offset++] << 48
           | buffer[offset++] << 56;
  }

  public static void writeLong(byte[] buffer, int offset, long l)
  {
    buffer[offset++] = (byte)l;
    buffer[offset++] = (byte)(l >> 8);
    buffer[offset++] = (byte)(l >> 16);
    buffer[offset++] = (byte)(l >> 24);
    buffer[offset++] = (byte)(l >> 32);
    buffer[offset++] = (byte)(l >> 40);
    buffer[offset++] = (byte)(l >> 48);
    buffer[offset++] = (byte)(l >> 56);
  }

  private static final Logger logger = LoggerFactory.getLogger(Server.class);
}
