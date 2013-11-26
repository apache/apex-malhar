/*
 *  Copyright (c) 2012-2013 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.datatorrent.flume.sink;

import java.io.IOException;
import java.net.InetSocketAddress;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.LoggerFactory;

import com.datatorrent.netlet.AbstractLengthPrependerClient;
import com.datatorrent.netlet.DefaultEventLoop;

/**
 *
 * @author Chetan Narsude <chetan@datatorrent.com>
 */
public class DTFlumeSinkTest
{
  DefaultEventLoop eventloop;

  @Before
  public void setup()
  {
    eventloop.start();
  }

  @After
  public void teardown()
  {
    eventloop.stop();
  }

  public DTFlumeSinkTest()
  {
    try {
      this.eventloop = new DefaultEventLoop("test-eventloop");
    }
    catch (IOException ex) {
      logger.error("eventloop exception", ex);
    }
  }

  @Test
  @SuppressWarnings("SleepWhileInLoop")
  public void testServer() throws InterruptedException
  {
    DTFlumeSink sink = new DTFlumeSink();
    sink.setEventloop(eventloop);
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
        write(DTFlumeSink.MESSAGES.COMMIT.toString().getBytes());
      }

    };

    eventloop.connect(new InetSocketAddress("localhost", 5033), client);
    synchronized (this) {
      this.wait();
    }

    sink.stop();
  }

  private static final org.slf4j.Logger logger = LoggerFactory.getLogger(DTFlumeSinkTest.class);
}