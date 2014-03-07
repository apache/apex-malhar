/*
 * Copyright (c) 2014 DataTorrent, Inc. ALL Rights Reserved.
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
package com.datatorrent.apps.logstream;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.junit.Assert;
import org.junit.Test;

import org.apache.commons.lang.mutable.MutableDouble;

import com.datatorrent.lib.logs.DimensionObject;
import com.datatorrent.lib.testbench.CollectorTestSink;

import com.datatorrent.apps.logstream.PropertyRegistry.LogstreamPropertyRegistry;
import com.datatorrent.common.util.DTThrowable;

/**
 *
 * Tests logstream dimension operator
 */
public class DimensionOperatorTest
{
  @Test
  @SuppressWarnings("unchecked")
  public void testOperator()
  {
    DimensionOperator oper = new DimensionOperator();
    LogstreamPropertyRegistry registry = new LogstreamPropertyRegistry();
    registry.bind(LogstreamUtil.LOG_TYPE, "apache");
    registry.bind(LogstreamUtil.FILTER, "ALL");
    oper.setRegistry(registry);
    // user input example::
    // type=apache,timebucket=m,timebucket=h,dimensions=a:b:c,dimensions=b:c,dimensions=b,dimensions=d,values=x.sum:y.sum:y.avg
    oper.addPropertiesFromString(new String[] {"type=apache", "timebucket=s", "dimensions=name", "dimensions=url", "dimensions=name:url", "values=value.sum:value.avg"});

    HashMap<String, Object> inMap1 = new HashMap<String, Object>();
    inMap1.put(LogstreamUtil.LOG_TYPE, registry.getIndex(LogstreamUtil.LOG_TYPE, "apache"));
    inMap1.put(LogstreamUtil.FILTER, registry.getIndex(LogstreamUtil.FILTER, "ALL"));
    inMap1.put("name", "abc");
    inMap1.put("url", "http://www.t.co");
    inMap1.put("value", 25);
    inMap1.put("response", "404");

    HashMap<String, Object> inMap2 = new HashMap<String, Object>();
    inMap2.put(LogstreamUtil.LOG_TYPE, registry.getIndex(LogstreamUtil.LOG_TYPE, "apache"));
    inMap2.put(LogstreamUtil.FILTER, registry.getIndex(LogstreamUtil.FILTER, "ALL"));
    inMap2.put("name", "xyz");
    inMap2.put("url", "http://www.t.co");
    inMap2.put("value", 25);
    inMap2.put("response", "404");

    HashMap<String, Object> inMap3 = new HashMap<String, Object>();
    inMap3.put(LogstreamUtil.LOG_TYPE, registry.getIndex(LogstreamUtil.LOG_TYPE, "apache"));
    inMap3.put(LogstreamUtil.FILTER, registry.getIndex(LogstreamUtil.FILTER, "ALL"));
    inMap3.put("name", "abc");
    inMap3.put("url", "http://www.t.co");
    inMap3.put("value", 25);
    inMap3.put("response", "404");

    HashMap<String, Object> inMap4 = new HashMap<String, Object>();
    inMap4.put(LogstreamUtil.LOG_TYPE, registry.getIndex(LogstreamUtil.LOG_TYPE, "apache"));
    inMap4.put(LogstreamUtil.FILTER, registry.getIndex(LogstreamUtil.FILTER, "ALL"));
    inMap4.put("name", "abc");
    inMap4.put("url", "http://www.t.co");
    inMap4.put("value", 25);
    inMap4.put("response", "404");

    CollectorTestSink mapSink = new CollectorTestSink();
    oper.aggregationsOutput.setSink(mapSink);

    long now = System.currentTimeMillis();
    long currentId = 0L;
    long windowId = (now / 1000) << 32 | currentId;
    oper.beginWindow(windowId);
    oper.in.process(inMap1);
    oper.in.process(inMap2);
    oper.in.process(inMap3);
    oper.in.process(inMap4);
    try {
      Thread.sleep(1000);
    }
    catch (Throwable ex) {
      DTThrowable.rethrow(ex);
    }

    oper.endWindow();

    currentId++;
    now = System.currentTimeMillis();
    windowId = (now / 1000) << 32 | currentId;
    oper.beginWindow(windowId);
    oper.endWindow();

    @SuppressWarnings("unchecked")
    List<Map<String, DimensionObject<String>>> tuples = mapSink.collectedTuples;

    for (Map<String, DimensionObject<String>> map : tuples) {
      for (Entry<String, DimensionObject<String>> entry : map.entrySet()) {
        String key = entry.getKey();
        DimensionObject<String> dimObj = entry.getValue();
        if (key.contains("COUNT")) {
          if (dimObj.getVal().equals("xyz")) {
            Assert.assertEquals("Count for key " + key, new MutableDouble(1), dimObj.getCount());
          }
          else if (dimObj.getVal().equals("abc")) {
            Assert.assertEquals("Count for key " + key, new MutableDouble(3), dimObj.getCount());
          }
          else if (dimObj.getVal().equals("abc,http://www.t.co")) {
            Assert.assertEquals("Count for key " + key, new MutableDouble(3), dimObj.getCount());
          }
          else if (dimObj.getVal().equals("xyz,http://www.t.co")) {
            Assert.assertEquals("Count for key " + key, new MutableDouble(1), dimObj.getCount());
          }
          else if (dimObj.getVal().equals("http://www.t.co")) {
            Assert.assertEquals("Count for key " + key, new MutableDouble(4), dimObj.getCount());
          }
          else {
            Assert.fail("Unexpected dimension object received: " + dimObj + " for key: " + key);
          }
        }
        else if (key.contains("AVERAGE")) {
          if (dimObj.getVal().equals("xyz")) {
            Assert.assertEquals("Count for key " + key, new MutableDouble(25), dimObj.getCount());
          }
          else if (dimObj.getVal().equals("abc")) {
            Assert.assertEquals("Count for key " + key, new MutableDouble(25), dimObj.getCount());
          }
          else if (dimObj.getVal().equals("abc,http://www.t.co")) {
            Assert.assertEquals("Count for key " + key, new MutableDouble(25), dimObj.getCount());
          }
          else if (dimObj.getVal().equals("xyz,http://www.t.co")) {
            Assert.assertEquals("Count for key " + key, new MutableDouble(25), dimObj.getCount());
          }
          else if (dimObj.getVal().equals("http://www.t.co")) {
            Assert.assertEquals("Count for key " + key, new MutableDouble(25), dimObj.getCount());
          }
          else {
            Assert.fail("Unexpected dimension object received: " + dimObj + " for key: " + key);
          }
        }
        else if (key.contains("SUM")) {
          if (dimObj.getVal().equals("xyz")) {
            Assert.assertEquals("Count for key " + key, new MutableDouble(25), dimObj.getCount());
          }
          else if (dimObj.getVal().equals("abc")) {
            Assert.assertEquals("Count for key " + key, new MutableDouble(75), dimObj.getCount());
          }
          else if (dimObj.getVal().equals("abc,http://www.t.co")) {
            Assert.assertEquals("Count for key " + key, new MutableDouble(75), dimObj.getCount());
          }
          else if (dimObj.getVal().equals("xyz,http://www.t.co")) {
            Assert.assertEquals("Count for key " + key, new MutableDouble(25), dimObj.getCount());
          }
          else if (dimObj.getVal().equals("http://www.t.co")) {
            Assert.assertEquals("Count for key " + key, new MutableDouble(100), dimObj.getCount());
          }
          else {
            Assert.fail("Unexpected dimension object received: " + dimObj + " for key: " + key);
          }
        }
        else {
          Assert.fail("Unexpected key received: " + key);
        }
      }
    }
  }

}
