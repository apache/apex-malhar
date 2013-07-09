/*
 * Copyright (c) 2013 Malhar Inc. ALL Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.datatorrent.lib.algo;

import java.util.Collections;
import java.util.Map;
import junit.framework.Assert;

import static org.junit.Assert.assertEquals;
import org.junit.Test;

import com.datatorrent.lib.algo.WindowedHolder;
import com.datatorrent.lib.algo.WindowedTopCounter;
import com.datatorrent.lib.util.JavaSerializationStreamCodec;
import com.datatorrent.common.util.Slice;
import com.datatorrent.lib.testbench.CollectorTestSink;

/**
 *
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
    Slice dsp = dsc.toByteArray(windowedHolder1);
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

    CollectorTestSink s = new CollectorTestSink();
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
