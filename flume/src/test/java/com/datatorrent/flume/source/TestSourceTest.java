/*
 *  Copyright (c) 2014 DataTorrent, Inc. ALL Rights Reserved.
 */
package com.datatorrent.flume.source;

import java.io.FileNotFoundException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;

import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.channel.ChannelProcessor;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 *
 * @author Chetan Narsude <chetan@datatorrent.com>
 */
public class TestSourceTest
{
  public static final String SOURCE_FILE = "src/test/resources/dt_spend";
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
    map.put(TestSource.SOURCE_FILE, SOURCE_FILE);
    map.put(TestSource.RATE, "50000");
    TestSource ts = new TestSource();
    try {
      ts.configure(new Context(map));
    } catch (RuntimeException re) {
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
      final int last = ts.cache.size();
      for (int i = 0; i < last; i++) {
        Event event =  queue.poll(2, TimeUnit.SECONDS);
        String row = new String(event.getBody());
        assertFalse("2013-11-07 should not be present", row.contains("2013-11-07"));
        assertFalse("2013-11-08 should not be present", row.contains("2013-11-08"));
        Map<String, String> headers = event.getHeaders();
        assertEquals("Source", SOURCE_FILE + i,
            headers.get(TestSource.SOURCE_FILE).concat(headers.get(TestSource.LINE_NUMBER)));
      }
    } finally {
      ts.stop();
    }
    long end = System.currentTimeMillis();

    assertTrue(String.format("Execution Time %d and %d", start,  end),
        end <= start + 500 + 1000 * ts.cache.size() / ts.rate);
  }

  private static final Logger logger = LoggerFactory.getLogger(TestSourceTest.class);
}
