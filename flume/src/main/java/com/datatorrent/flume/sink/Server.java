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
import java.util.Arrays;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datatorrent.flume.discovery.Discovery;
import com.datatorrent.flume.discovery.Discovery.Service;
import com.datatorrent.netlet.AbstractLengthPrependerClient;
import com.datatorrent.netlet.AbstractServer;
import com.datatorrent.netlet.EventLoop;
import com.datatorrent.netlet.util.Slice;

/**
 * <p>
 * Server class.</p>
 *
 * @author Chetan Narsude <chetan@datatorrent.com>
 * @since 0.9.2
 */
public class Server extends AbstractServer
{
  private final String id;
  private final Discovery<byte[]> discovery;
  private final long acceptedTolerance;

  public Server(String id, Discovery<byte[]> discovery, long acceptedTolerance)
  {
    this.id = id;
    this.discovery = discovery;
    this.acceptedTolerance = acceptedTolerance;
  }

  @Override
  public void handleException(Exception cce, EventLoop el)
  {
    logger.error("Server Error", cce);
    Request r = new Request(Command.SERVER_ERROR, null)
    {
      @Override
      public Slice getAddress()
      {
        throw new UnsupportedOperationException("Not supported yet.");
      }

      @Override
      public int getEventCount()
      {
        throw new UnsupportedOperationException("Not supported yet.");
      }

      @Override
      public int getIdleCount()
      {
        throw new UnsupportedOperationException("Not supported yet.");
      }

    };
    synchronized (requests) {
      requests.add(r);
    }
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

    @Override
    public String toString()
    {
      return "Server.Service{id=" + id + ", host=" + getHost() + ", port=" + getPort() + ", payload=" +
          Arrays.toString(getPayload()) + '}';
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
    SEEK((byte)1),
    COMMITTED((byte)2),
    CHECKPOINTED((byte)3),
    CONNECTED((byte)4),
    DISCONNECTED((byte)5),
    WINDOWED((byte)6),
    SERVER_ERROR((byte)7);

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
      Command c;
      switch (b) {
        case 0:
          c = ECHO;
          break;

        case 1:
          c = SEEK;
          break;

        case 2:
          c = COMMITTED;
          break;

        case 3:
          c = CHECKPOINTED;
          break;

        case 4:
          c = CONNECTED;
          break;

        case 5:
          c = DISCONNECTED;
          break;

        case 6:
          c = WINDOWED;
          break;

        case 7:
          c = SERVER_ERROR;
          break;

        default:
          throw new IllegalArgumentException(String.format("No Command defined for ordinal %b", b));
      }

      assert (b == c.ord);
      return c;
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

    @Override
    public void onMessage(byte[] buffer, int offset, int size)
    {
      if (size != Request.FIXED_SIZE) {
        logger.warn("Invalid Request Received: {} from {}", Arrays.copyOfRange(buffer, offset, offset + size),
            key.channel());
        return;
      }

      long requestTime = Server.readLong(buffer, offset + Request.TIME_OFFSET);
      if (System.currentTimeMillis() > (requestTime + acceptedTolerance)) {
        logger.warn("Expired Request Received: {} from {}", Arrays.copyOfRange(buffer, offset, offset + size),
            key.channel());
        return;
      }

      try {
        if (Command.getCommand(buffer[offset]) == Command.ECHO) {
          write(buffer, offset, size);
          return;
        }
      } catch (IllegalArgumentException ex) {
        logger.warn("Invalid Request Received: {} from {}!", Arrays.copyOfRange(buffer, offset, offset + size),
            key.channel(), ex);
        return;
      }

      Request r = Request.getRequest(buffer, offset, this);
      synchronized (requests) {
        requests.add(r);
      }
    }

    @Override
    public void disconnected()
    {
      synchronized (requests) {
        requests.add(Request.getRequest(
            new byte[] {Command.DISCONNECTED.getOrdinal(), 0, 0, 0, 0, 0, 0, 0, 0}, 0, this));
      }
      super.disconnected();
    }

    public boolean write(byte[] address, Slice event)
    {
      if (event.offset == 0 && event.length == event.buffer.length) {
        return write(address, event.buffer);
      }

      // a better method would be to replace the write implementation and allow it to natively support writing slices
      return write(address, event.toByteArray());
    }

  }

  public abstract static class Request
  {
    public static final int FIXED_SIZE = 17;
    public static final int TIME_OFFSET = 9;
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
