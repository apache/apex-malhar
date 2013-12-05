/*
 *  Copyright (c) 2012-2013 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.datatorrent.flume.sink;

import java.io.IOException;
import java.net.InetSocketAddress;

import org.junit.Test;
import org.slf4j.LoggerFactory;

import com.datatorrent.netlet.AbstractLengthPrependerClient;
import com.datatorrent.netlet.DefaultEventLoop;
import org.apache.flume.channel.MemoryChannel;

/**
 *
 * @author Chetan Narsude <chetan@datatorrent.com>
 */
public class DTFlumeSinkTest
{
  static final String hostname = "localhost";
  static final int port = 5033;

  @Test
  @SuppressWarnings("SleepWhileInLoop")
  public void testServer() throws InterruptedException, IOException
  {
    DTFlumeSink sink = new DTFlumeSink();
    sink.setName("TeskSink");
    sink.setHostname(hostname);
    sink.setPort(port);
    sink.setChannel(new MemoryChannel());
    sink.start();
    AbstractLengthPrependerClient client = new AbstractLengthPrependerClient()
    {
      @Override
      public void onMessage(byte[] buffer, int offset, int size)
      {
        logger.debug("Client Received = {}", new String(buffer, offset, size));
        synchronized (DTFlumeSinkTest.this) {
          DTFlumeSinkTest.this.notify();
        }
      }

      @Override
      public void connected()
      {
        super.connected();
        byte[] array = new byte[12];
        array[0] = Server.Command.ECHO.getOrdinal();
        array[1] = 1;
        array[2] = 2;
        array[3] = 3;
        array[4] = 4;
        array[5] = 5;
        array[6] = 6;
        array[7] = 7;
        array[8] = 8;
        array[9] = 9;
        array[10] = 10;
        array[11] = 11;
        write(array);
      }

    };

    DefaultEventLoop eventloop = new DefaultEventLoop("Eventloop-TestClient");
    eventloop.start();
    try {
      eventloop.connect(new InetSocketAddress(hostname, port), client);
      try {
        synchronized (this) {
          this.wait();
        }
      }
      finally {
        eventloop.disconnect(client);
      }
    }
    finally {
      eventloop.stop();
    }

    sink.stop();
  }

  private static final org.slf4j.Logger logger = LoggerFactory.getLogger(DTFlumeSinkTest.class);
}