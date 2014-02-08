/*
 *  Copyright (c) 2014 DataTorrent, Inc. ALL Rights Reserved.
 */
package com.datatorrent.flume.source;

import java.io.FileNotFoundException;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;

import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import static org.junit.Assert.*;

import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.channel.ChannelProcessor;

/**
 *
 * @author Chetan Narsude <chetan@datatorrent.com>
 */
public class TestSourceTest
{
  public static class MyChannelProcessor extends ChannelProcessor
  {
    private final BlockingQueue<Event> queue;

    public MyChannelProcessor(BlockingQueue<Event> queue)
    {
      super(null);
      this.queue = queue;
    }

    @Override
    public void processEventBatch(List<Event> events)
    {
      queue.addAll(events);
    }

  }

  @Test
  public void testSource() throws InterruptedException
  {
    HashMap<String, String> map = new HashMap<String, String>();
    map.put(TestSource.FILE_NAME, "src/test/resources/dt_spend");
    map.put(TestSource.RATE, "50000");
    TestSource ts = new TestSource();
    try {
      ts.configure(new Context(map));
    }
    catch (RuntimeException re) {
      if (re.getCause() instanceof FileNotFoundException) {
        logger.warn("Skipping testSource as {}", re.getCause().getLocalizedMessage());
        return;
      }

      throw re;
    }

    BlockingQueue<Event> queue = new ArrayBlockingQueue<Event>(ts.cache.size());
    ts.setChannelProcessor(new MyChannelProcessor(queue));

    long start = System.currentTimeMillis();
    ts.start();
    try {
      for (int i = ts.cache.size(); i-- > 0;) {
        assertNotNull(queue.poll(2, TimeUnit.SECONDS));
      }
    }
    finally {
      ts.stop();
    }
    long end = System.currentTimeMillis();

    assertTrue(String.format("Execution Time %d and %d", start,  end), end <= start + 500 + 1000 * ts.cache.size() / ts.rate);
  }

  private static final Logger logger = LoggerFactory.getLogger(TestSourceTest.class);
}