/*
 *  Copyright (c) 2012 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.malhartech.lib.algo;

import java.util.Collections;
import java.util.Map;
import junit.framework.Assert;

import static org.junit.Assert.assertEquals;
import org.junit.Test;

import com.malhartech.codec.JavaSerializationStreamCodec;
import com.malhartech.common.Fragment;
import com.malhartech.engine.TestSink;

/**
 *
 * @author Chetan Narsude <chetan@malhar-inc.com>
 */
public class WindowedHolderTest
{
  public WindowedHolderTest()
  {
  }

  @Test
  public void testKryo()
  {
    WindowedHolder<String> windowedHolder1 = new WindowedHolder<String>("one", 2);
    windowedHolder1.adjustCount(1);
    windowedHolder1.slide();
    assertEquals("total count", windowedHolder1.totalCount, 1);
    windowedHolder1.adjustCount(2);
    windowedHolder1.slide();
    assertEquals("total count", windowedHolder1.totalCount, 2);
    windowedHolder1.adjustCount(3);

    assertEquals("count at exiting position", windowedHolder1.windowedCount[0], 3);
    assertEquals("count at new position", windowedHolder1.windowedCount[1], 2);


    JavaSerializationStreamCodec<WindowedHolder<String>> dsc = new JavaSerializationStreamCodec<WindowedHolder<String>>();
    Fragment dsp = dsc.toByteArray(windowedHolder1);
    @SuppressWarnings("unchecked")
    WindowedHolder<String> windowedHolder2 = (WindowedHolder<String>)dsc.fromByteArray(dsp);

    assertEquals("count at exiting position", windowedHolder2.windowedCount[0], 3);
    assertEquals("count at new position", windowedHolder2.windowedCount[1], 2);
    assertEquals("total count", windowedHolder2.totalCount, 2);
  }

  @Test
  public void testCountToZero()
  {
    WindowedHolder<String> windowedHolder1 = new WindowedHolder<String>("one", 3);
    windowedHolder1.adjustCount(2);
    windowedHolder1.slide();
    assertEquals("total count", windowedHolder1.totalCount, 2);
    windowedHolder1.slide();
    assertEquals("total count", windowedHolder1.totalCount, 2);
    windowedHolder1.slide();
    assertEquals("total count", windowedHolder1.totalCount, 0);
    windowedHolder1.slide();
    assertEquals("total count", windowedHolder1.totalCount, 0);
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testWindowedTopCounter() {
    WindowedTopCounter<String> topCounts = new WindowedTopCounter<String>();
    topCounts.setTopCount(10);
    topCounts.setSlidingWindowWidth(6, 1);

    TestSink s = new TestSink();
    topCounts.output.setSink(s);

    int windowId = 1;
    topCounts.setup(null);
    topCounts.beginWindow(windowId);
    topCounts.input.process(Collections.singletonMap("key1", 5));
    topCounts.endWindow();

    Assert.assertEquals(""+s.collectedTuples, Integer.valueOf(5), ((Map<String, Integer>)s.collectedTuples.get(0)).get("key1"));

    while (windowId < 6) {
      s.clear();
      topCounts.beginWindow(++windowId);
      topCounts.endWindow();
      Assert.assertEquals("window " + windowId + " " + s.collectedTuples, Integer.valueOf(5), ((Map<String, Integer>)s.collectedTuples.get(0)).get("key1"));
    }

    s.clear();
    topCounts.beginWindow(++windowId);
    topCounts.endWindow();
    Assert.assertEquals(" " + s.collectedTuples, null, ((Map<String, Integer>)s.collectedTuples.get(0)).get("key1"));
  }

}
