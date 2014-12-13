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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.Assert;
import org.junit.Test;

import org.apache.commons.lang.mutable.MutableDouble;

import com.datatorrent.lib.logs.DimensionObject;
import com.datatorrent.lib.testbench.CollectorTestSink;

import com.datatorrent.apps.logstream.PropertyRegistry.LogstreamPropertyRegistry;

/**
 *
 * Tests logstream topn operator.
 */
public class LogstreamTopNTest
{
  @Test
  @SuppressWarnings("unchecked")
  public void testOperator()
  {
    LogstreamTopN oper = new LogstreamTopN();
    LogstreamPropertyRegistry registry = new LogstreamPropertyRegistry();
    registry.bind(LogstreamUtil.LOG_TYPE, "apache");
    registry.bind(LogstreamUtil.FILTER, "default");
    oper.setRegistry(registry);
    oper.setN(5);

    CollectorTestSink mapSink = new CollectorTestSink();
    oper.top.setSink(mapSink);

    oper.beginWindow(0);
    Map<String, DimensionObject<String>> tuple1 = new HashMap<String, DimensionObject<String>>();
    DimensionObject<String> dimObja = new DimensionObject<String>(new MutableDouble(10), "a");
    tuple1.put("m|201402121900|0|65535|131075|val.COUNT", dimObja);
    oper.data.process(tuple1);

    DimensionObject<String> dimObjb = new DimensionObject<String>(new MutableDouble(1), "b");
    tuple1.put("m|201402121900|0|65535|131075|val.COUNT", dimObjb);
    oper.data.process(tuple1);

    DimensionObject<String> dimObjc = new DimensionObject<String>(new MutableDouble(5), "c");
    tuple1.put("m|201402121900|0|65535|131075|val.COUNT", dimObjc);
    oper.data.process(tuple1);

    DimensionObject<String> dimObjd = new DimensionObject<String>(new MutableDouble(2), "d");
    tuple1.put("m|201402121900|0|65535|131075|val.COUNT", dimObjd);
    oper.data.process(tuple1);

    DimensionObject<String> dimObje = new DimensionObject<String>(new MutableDouble(15), "e");
    tuple1.put("m|201402121900|0|65535|131075|val.COUNT", dimObje);
    oper.data.process(tuple1);

    DimensionObject<String> dimObjf = new DimensionObject<String>(new MutableDouble(4), "f");
    tuple1.put("m|201402121900|0|65535|131075|val.COUNT", dimObjf);
    oper.data.process(tuple1);

    oper.endWindow();

    @SuppressWarnings("unchecked")
    Map<String, List<DimensionObject<String>>> tuples = (Map<String, List<DimensionObject<String>>>)mapSink.collectedTuples.get(0);
    List<DimensionObject<String>> outList = tuples.get("m|201402121900|0|65535|131075|val.COUNT");

    List<DimensionObject<String>> expectedList = new ArrayList<DimensionObject<String>>();
    expectedList.add(dimObje);
    expectedList.add(dimObja);
    expectedList.add(dimObjc);
    expectedList.add(dimObjf);
    expectedList.add(dimObjd);

    Assert.assertEquals("Size", expectedList.size(), outList.size());
    Assert.assertEquals("compare list", expectedList, outList);
  }

}
