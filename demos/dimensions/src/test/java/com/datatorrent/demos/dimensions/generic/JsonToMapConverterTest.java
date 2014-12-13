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

package com.datatorrent.demos.dimensions.generic;

import com.datatorrent.lib.testbench.CollectorTestSink;
import org.junit.Assert;
import org.junit.Test;
import java.util.HashMap;
import java.util.Map;



/**
 * Test {@link JsonToMapConverter}
 */
public class JsonToMapConverterTest {
  @SuppressWarnings({ "rawtypes", "unchecked" })
  @Test
  public void testOperator() throws Exception {
    JsonToMapConverter operator = new JsonToMapConverter();
    CollectorTestSink mapSink = new CollectorTestSink();
    operator.outputMap.setSink(mapSink);

    operator.beginWindow(0);

    // Sample values
    Map<String, Object> data = new HashMap<String, Object>();
    data.put("publisherId", new Integer(1));
    data.put("advertiserId", new Integer(2));
    data.put("adUnit", new Integer(3));
    data.put("timestamp", new Long(1411432560000L));
    data.put("cost", new Double(53216.10593373731));
    data.put("revenue", new Double(312.08491273524476));
    data.put("impressions", new Long(85120L));
    data.put("clicks", new Long(1419L));

    // Sample input string in JSON format
    StringBuilder json = new StringBuilder();
    json.append("{");
    for (String key : data.keySet()) {
      json.append("\"").append(key).append("\": ").append(data.get(key)).append(",");
    }
    json.append("\"test\": true").append("}");

    byte[] inputByteArray = json.toString().getBytes();

    // run the operator 100 times
    int numTuples = 100;
    for (int i = 0; i < numTuples; i++) {
      operator.input.process(inputByteArray);
    }

    operator.endWindow();

    // validate number of tuples emitted
    Assert.assertEquals("number emitted tuples", numTuples, mapSink.collectedTuples.size());

    // validate the data inside the emitted map.  Data types may not match up, so primitive
    Map<String, Number> testTuple = (Map) mapSink.collectedTuples.get(51);
    for (String key : data.keySet()) {
      Number value = (Number)data.get(key);
      if (value instanceof Integer) {
        Assert.assertEquals("emitted map." + key, value.intValue(), testTuple.get(key).intValue());
      } else if (value instanceof Long) {
        Assert.assertEquals("emitted map." + key, value.longValue(), testTuple.get(key).longValue());
      } else if (value instanceof Float) {
        Assert.assertEquals("emitted map." + key, value.floatValue(), testTuple.get(key).floatValue(), 0);
      } else if (value instanceof Double) {
        Assert.assertEquals("emitted map." + key, value.doubleValue(), testTuple.get(key).doubleValue(), 0);
      } else {
        Assert.assertEquals("emitted map." + key, value, testTuple.get(key));
      }
    }
  }
}

