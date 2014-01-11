/*
 *  Copyright (c) 2012-2013 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.datatorrent.flume.sink;

import java.net.InetSocketAddress;
import java.nio.channels.SelectionKey;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.util.ArrayList;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datatorrent.common.util.Slice;
import com.datatorrent.flume.discovery.Discovery;
import com.datatorrent.flume.discovery.Discovery.Service;
import com.datatorrent.netlet.AbstractLengthPrependerClient;

/**
 *
 * @author Chetan Narsude <chetan@datatorrent.com>
 */
public class Server extends com.datatorrent.netlet.Server
{
  private final String id;
  private final Discovery<byte[]> discovery;

  public Server(String id, Discovery<byte[]> discovery)
  {
    this.id = id;
    this.discovery = discovery;
  }

  private final Service<byte[]> service = new Service<byte[]>()
  {
    @Override
    public String getHost()
    {
        return ((InetSocketAddress)getServerAddress()).getHostName();
    }

    @Override
    public int getPort()
    {
        return ((InetSocketAddress)getServerAddress()).getPort();
    }

    @Override
    public byte[] getPayload()
    {
        return null;
    }

    @Override
    public String getId()
    {
        return id;
    }

  };

  @Override
  public void unregistered(final SelectionKey key)
  {
    discovery.unadvertise(service);
    super.unregistered(key);
  }

  @Override
  public void registered(final SelectionKey key)
  {
    super.registered(key);
    discovery.advertise(service);
  }

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

    private final byte ord;
  }

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
      logger.debug("got a message with id {} of size {}", buffer[offset], size);
      if (Command.getCommand(buffer[offset]) == Command.ECHO) {
        write(buffer, offset, size);
        return;
      }

      logger.debug("adding to the requeusts queue");
      Request r = Request.getRequest(buffer, offset, this);
      synchronized (requests) {
        requests.add(r);
      }
    }

    @Override
    public void connected()
    {
      super.connected();
      logger.debug("some client connected!");
    }

    @Override
    public void disconnected()
    {
      synchronized (requests) {
        requests.add(Request.getRequest(new byte[] {Command.DISCONNECTED.getOrdinal(), 0, 0, 0, 0, 0, 0, 0, 0}, 0, this));
      }
      super.disconnected();
    }

  }

  public static abstract class Request
  {
    public final Command type;
    public final Client client;

    public Request(Command type, Client client)
    {
      this.type = type;
      this.client = client;
    }

    public abstract Slice getAddress();

    public abstract int getEventCount();

    public abstract int getIdleCount();

    @Override
    public String toString()
    {
      return "Request{" + "type=" + type + '}';
    }

    public static Request getRequest(final byte[] buffer, final int offset, Client client)
    {
      Command command = Command.getCommand(buffer[offset]);
      switch (command) {
        case WINDOWED:
          return new Request(Command.WINDOWED, client)
          {
            final int eventCount;
            final int idleCount;

            {
              eventCount = Server.readInt(buffer, offset + 1);
              idleCount = Server.readInt(buffer, offset + 5);
            }

            @Override
            public Slice getAddress()
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
          return new Request(command, client)
          {
            final Slice address;

            {
              address = new Slice(buffer, offset + 1, 8);
            }

            @Override
            public Slice getAddress()
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
