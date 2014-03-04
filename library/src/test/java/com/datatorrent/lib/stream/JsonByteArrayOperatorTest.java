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

import java.util.Map;
import junit.framework.Assert;

import org.codehaus.jettison.json.JSONObject;
import org.junit.Test;

import com.datatorrent.lib.testbench.CollectorTestSink;

/**
 * Tests JsonByteArrayOperator operator
 *
 */
public class JsonByteArrayOperatorTest
{
   /**
     * Test json byte array to HashMap operator pass through. The Object passed is not relevant
     */
    @SuppressWarnings({ "rawtypes", "unchecked" })
		@Test
    public void testOperator() throws Exception
    {
      JsonByteArrayOperator oper = new JsonByteArrayOperator();
      oper.setConcatenationCharacter('.');

      CollectorTestSink mapSink = new CollectorTestSink();
      CollectorTestSink jsonObjectSink = new CollectorTestSink();
      CollectorTestSink flatMapSink = new CollectorTestSink();

      oper.outputMap.setSink(mapSink);
      oper.outputJsonObject.setSink(jsonObjectSink);
      oper.outputFlatMap.setSink(flatMapSink);

      oper.beginWindow(0);

      // input test json string
      String inputJson  = " {   \"@timestamp\":\"2013-09-25T19:37:23.569Z\""
                        + "      ,\"@version\":\"1\""
                        + "          ,\"type\":\"apache-logs\""
                        + "          ,\"host\":\"node1001\""
                        + "      ,\"clientip\":192.168.150.120"
                        + "          ,\"verb\":\"GET\""
                        + "       ,\"request\":\"/reset.css\""
                        + "   ,\"httpversion\":\"1.1\""
                        + "      ,\"response\":200"
                        + "     ,\"agentinfo\": {\"browser\":Firefox"
                        + "                          ,\"os\": {    \"name\":\"Ubuntu\""
                        + "                                    ,\"version\":\"10.04\""
                        + "                                   }"
                        + "                     }"
                        + "         ,\"bytes\":909.1"
                        + " }";

      byte[] inputByteArray = inputJson.getBytes();

      // run the operator for the same string 1000 times
      int numtuples = 1000;
      for (int i = 0; i < numtuples; i++) {
        oper.input.process(inputByteArray);
      }

      oper.endWindow();

      // assert that the number of the operator generates is 1000
      Assert.assertEquals("number emitted tuples", numtuples, mapSink.collectedTuples.size());
      Assert.assertEquals("number emitted tuples", numtuples, jsonObjectSink.collectedTuples.size());
      Assert.assertEquals("number emitted tuples", numtuples, flatMapSink.collectedTuples.size());

      // assert that value for one of the keys in any one of the objects from mapSink is as expected
      Object map = mapSink.collectedTuples.get(510);
      String expectedClientip = "192.168.150.120";
      Assert.assertEquals("emitted tuple", expectedClientip, ((Map)map).get("clientip"));


      // assert that value for one of the keys in any one of the objects from jsonObjectSink is as expected
      Object jsonObject = jsonObjectSink.collectedTuples.get(433);
      Number expectedResponse = 200;
      Assert.assertEquals("emitted tuple", expectedResponse, ((JSONObject)jsonObject).get("response"));

      // assert that value for one of the keys in any one of the objects from flatMapSink is as expected
      Map flatMap = (Map)flatMapSink.collectedTuples.get(511);
      String expectedBrowser = "Firefox";
      String expectedOsName = "Ubuntu";
      Assert.assertEquals("emitted tuple", expectedBrowser, flatMap.get("agentinfo.browser"));
      Assert.assertEquals("emitted tuple", expectedOsName, flatMap.get("agentinfo.os.name"));
    }

}
