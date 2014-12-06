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

/**
 *
 * Tests logstream dimension operator unifier.
 */
public class DimensionOperatorUnifierTest
{
  @Test
  @SuppressWarnings("unchecked")
  public void testOperator()
  {
    DimensionOperatorUnifier unifier = new DimensionOperatorUnifier();
    CollectorTestSink sink = new CollectorTestSink();

    unifier.aggregationsOutput.setSink(sink);

    unifier.beginWindow(1);

    Map<String, DimensionObject<String>> tuple1 = new HashMap<String, DimensionObject<String>>();

    tuple1.put("m|201402121900|0|65537|131074|bytes.AVERAGE", new DimensionObject<String>(new MutableDouble(75), "a"));
    tuple1.put("m|201402121900|0|65537|131074|bytes.COUNT", new DimensionObject<String>(new MutableDouble(3.0), "a"));
    tuple1.put("m|201402121900|0|65537|131074|bytes.SUM", new DimensionObject<String>(new MutableDouble(225), "a"));

    Map<String, DimensionObject<String>> tuple2 = new HashMap<String, DimensionObject<String>>();

    tuple2.put("m|201402121900|0|65537|131074|bytes.AVERAGE", new DimensionObject<String>(new MutableDouble(50), "a"));
    tuple2.put("m|201402121900|0|65537|131074|bytes.COUNT", new DimensionObject<String>(new MutableDouble(2.0), "a"));
    tuple2.put("m|201402121900|0|65537|131074|bytes.SUM", new DimensionObject<String>(new MutableDouble(100), "a"));

    Map<String, DimensionObject<String>> tuple3 = new HashMap<String, DimensionObject<String>>();

    tuple3.put("m|201402121900|0|65537|131074|bytes.AVERAGE", new DimensionObject<String>(new MutableDouble(50), "z"));
    tuple3.put("m|201402121900|0|65537|131074|bytes.COUNT", new DimensionObject<String>(new MutableDouble(2.0), "z"));
    tuple3.put("m|201402121900|0|65537|131074|bytes.SUM", new DimensionObject<String>(new MutableDouble(100), "z"));

    Map<String, DimensionObject<String>> tuple4 = new HashMap<String, DimensionObject<String>>();

    tuple4.put("m|201402121900|0|65537|131075|bytes.AVERAGE", new DimensionObject<String>(new MutableDouble(14290.5), "b"));
    tuple4.put("m|201402121900|0|65537|131075|bytes.COUNT", new DimensionObject<String>(new MutableDouble(2.0), "b"));
    tuple4.put("m|201402121900|0|65537|131075|bytes.SUM", new DimensionObject<String>(new MutableDouble(28581.0), "b"));

    Map<String, DimensionObject<String>> tuple5 = new HashMap<String, DimensionObject<String>>();

    tuple5.put("m|201402121900|0|65537|131076|bytes.AVERAGE", new DimensionObject<String>(new MutableDouble(290.75), "c"));
    tuple5.put("m|201402121900|0|65537|131076|bytes.COUNT", new DimensionObject<String>(new MutableDouble(10.0), "c"));
    tuple5.put("m|201402121900|0|65537|131076|bytes.SUM", new DimensionObject<String>(new MutableDouble(8581.0), "c"));

    unifier.process(tuple1);
    unifier.process(tuple2);
    unifier.process(tuple3);
    unifier.process(tuple4);
    unifier.process(tuple5);

    unifier.endWindow();

    @SuppressWarnings("unchecked")
    List<Map<String, DimensionObject<String>>> tuples = sink.collectedTuples;

    Assert.assertEquals("Tuple Count", 4, tuples.size());

    for (Map<String, DimensionObject<String>> map : tuples) {
      for (Entry<String, DimensionObject<String>> entry : map.entrySet()) {
        String key = entry.getKey();
        DimensionObject<String> dimObj = entry.getValue();
        if (key.equals("m|201402121900|0|65537|131074|bytes.AVERAGE") && dimObj.getVal().equals("a")) {
          Assert.assertEquals("average for key " + key + " and dimension key " + "a", new MutableDouble(65), dimObj.getCount());
        }

        if (key.equals("m|201402121900|0|65537|131074|bytes.SUM") && dimObj.getVal().equals("z")) {
          Assert.assertEquals("sum for key " + key + " and dimension key " + "z", new MutableDouble(100), dimObj.getCount());
        }

        if (key.equals("m|201402121900|0|65537|131076|bytes.COUNT") && dimObj.getVal().equals("c")) {
          Assert.assertEquals("count for key " + key + " and dimension key " + "c", new MutableDouble(10), dimObj.getCount());
        }
      }
    }
  }

}
