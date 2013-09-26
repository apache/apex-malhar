/*
 * Copyright (c) 2013 DataTorrent, Inc. ALL Rights Reserved.
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
package com.datatorrent.lib.stream;

import com.datatorrent.lib.testbench.CollectorTestSink;
import java.util.Map;
import junit.framework.Assert;
import org.junit.Test;

/**
 * Tests JsonByteArrayToHashMap operator
 *
 */
public class JsonByteArrayToHashMapTest
{
   /**
     * Test json byte array to HashMap operator pass through. The Object passed is not relevant
     */
    @SuppressWarnings({ "rawtypes", "unchecked" })
		@Test
    public void testOperator() throws Exception
    {
      JsonByteArrayToHashMap oper = new JsonByteArrayToHashMap();

      CollectorTestSink itemSink = new CollectorTestSink();
      oper.outputMap.setSink(itemSink);

      oper.beginWindow(0);

      // input test json string
      String inputJson  = "{\"@timestamp\":\"2013-09-25T19:37:23.569Z\""
                        + ",\"@version\":\"1\""
                        + ",\"type\":\"apache-logs\""
                        + ",\"host\":\"node1001\""
                        + ",\"clientip\":\"192.168.150.120\""
                        + ",\"verb\":\"GET\""
                        + ",\"request\":\"/reset.css\""
                        + ",\"httpversion\":\"1.1\""
                        + ",\"response\":\"200\""
                        + ",\"bytes\":\"909\"}";

      byte[] inputByteArray = inputJson.getBytes();

      // run the operator for the same string 1000 times
      int numtuples = 1000;
      for (int i = 0; i < numtuples; i++) {
        oper.input.process(inputByteArray);
      }

      oper.endWindow();

      // assert that value for one of the keys in any one of the objects is as expected
      Object o = itemSink.collectedTuples.get(510);
      String expectedClientip = "192.168.150.120";
      Assert.assertEquals("emited tuple", expectedClientip, ((Map)o).get("clientip"));

      // assert that the number of the operator generates is 1000
      Assert.assertEquals("number emitted tuples", numtuples, itemSink.collectedTuples.size());
    }

}
