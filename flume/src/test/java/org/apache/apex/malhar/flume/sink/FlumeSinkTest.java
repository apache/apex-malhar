/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.apex.malhar.flume.sink;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.apex.malhar.flume.discovery.Discovery;

import org.apache.flume.channel.MemoryChannel;

import com.datatorrent.netlet.AbstractLengthPrependerClient;
import com.datatorrent.netlet.DefaultEventLoop;
import com.datatorrent.netlet.util.Slice;

/**
 *
 */
public class FlumeSinkTest
{
  static final String hostname = "localhost";
  int port = 0;

  @Test
  @SuppressWarnings("SleepWhileInLoop")
  public void testServer() throws InterruptedException, IOException
  {
    final CountDownLatch countdown = new CountDownLatch(1);
    Discovery<byte[]> discovery = new Discovery<byte[]>()
    {
      @Override
      public void unadvertise(Service<byte[]> service)
      {
        logger.info("Unadvertise invoked");
        countdown.countDown();
      }

      @Override
      public void advertise(Service<byte[]> service)
      {
        logger.info("Advertise invoked");
        port = service.getPort();
        logger.debug("listening at {}", service);
        countdown.countDown();
      }

      @Override
      @SuppressWarnings("unchecked")
      public synchronized Collection<Service<byte[]>> discover()
      {
        logger.info("Discover invoked");
        try {
          countdown.await(30, TimeUnit.SECONDS);
          logger.info("Discover wait completed");
        } catch (InterruptedException ie) {
          throw new RuntimeException(ie);
        }
        return Collections.EMPTY_LIST;
      }

    };
    DefaultEventLoop eventloop = new DefaultEventLoop("Eventloop-TestClient");
    eventloop.start();
    FlumeSink sink = new FlumeSink();
    sink.setName("TeskSink");
    sink.setHostname(hostname);
    sink.setPort(0);
    sink.setAcceptedTolerance(2000);
    sink.setChannel(new MemoryChannel());
    sink.setDiscovery(discovery);
    sink.start();
    logger.info("Sink started");
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
        synchronized (FlumeSinkTest.this) {
          FlumeSinkTest.this.notify();
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
        logger.info("Connect complete");
      }

    };
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

  private static final Logger logger = LoggerFactory.getLogger(FlumeSinkTest.class);
}
