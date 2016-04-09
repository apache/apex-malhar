/*
 *  Copyright (c) 2012-2013 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.datatorrent.flume.sink;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;

import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.flume.channel.MemoryChannel;

import com.datatorrent.flume.discovery.Discovery;
import com.datatorrent.netlet.AbstractLengthPrependerClient;
import com.datatorrent.netlet.DefaultEventLoop;
import com.datatorrent.netlet.util.Slice;

/**
 * @author Chetan Narsude <chetan@datatorrent.com>
 */
public class DTFlumeSinkTest
{
  static final String hostname = "localhost";
  int port = 0;

  @Test
  @SuppressWarnings("SleepWhileInLoop")
  public void testServer() throws InterruptedException, IOException
  {
    Discovery<byte[]> discovery = new Discovery<byte[]>()
    {
      @Override
      public synchronized void unadvertise(Service<byte[]> service)
      {
        notify();
      }

      @Override
      public synchronized void advertise(Service<byte[]> service)
      {
        port = service.getPort();
        logger.debug("listening at {}", service);
        notify();
      }

      @Override
      @SuppressWarnings("unchecked")
      public synchronized Collection<Service<byte[]>> discover()
      {
        try {
          wait();
        } catch (InterruptedException ie) {
          throw new RuntimeException(ie);
        }
        return Collections.EMPTY_LIST;
      }

    };
    DTFlumeSink sink = new DTFlumeSink();
    sink.setName("TeskSink");
    sink.setHostname(hostname);
    sink.setPort(0);
    sink.setAcceptedTolerance(2000);
    sink.setChannel(new MemoryChannel());
    sink.setDiscovery(discovery);
    sink.start();
    AbstractLengthPrependerClient client = new AbstractLengthPrependerClient()
    {
      private byte[] array;
      private int offset = 2;

      @Override
      public void onMessage(byte[] buffer, int offset, int size)
      {
        Slice received = new Slice(buffer, offset, size);
        logger.debug("Client Received = {}", received);
        Assert.assertEquals(received,
            new Slice(Arrays.copyOfRange(array, this.offset, array.length), 0, Server.Request.FIXED_SIZE));
        synchronized (DTFlumeSinkTest.this) {
          DTFlumeSinkTest.this.notify();
        }
      }

      @Override
      public void connected()
      {
        super.connected();
        array = new byte[Server.Request.FIXED_SIZE + offset];
        array[offset] = Server.Command.ECHO.getOrdinal();
        array[offset + 1] = 1;
        array[offset + 2] = 2;
        array[offset + 3] = 3;
        array[offset + 4] = 4;
        array[offset + 5] = 5;
        array[offset + 6] = 6;
        array[offset + 7] = 7;
        array[offset + 8] = 8;
        Server.writeLong(array, offset + Server.Request.TIME_OFFSET, System.currentTimeMillis());
        write(array, offset, Server.Request.FIXED_SIZE);
      }

    };

    DefaultEventLoop eventloop = new DefaultEventLoop("Eventloop-TestClient");
    eventloop.start();
    discovery.discover();
    try {
      eventloop.connect(new InetSocketAddress(hostname, port), client);
      try {
        synchronized (this) {
          this.wait();
        }
      } finally {
        eventloop.disconnect(client);
      }
    } finally {
      eventloop.stop();
    }

    sink.stop();
  }

  private static final Logger logger = LoggerFactory.getLogger(DTFlumeSinkTest.class);
}
