/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.apex.malhar.contrib.r;

import java.util.HashMap;
import java.util.Map;

import org.junit.Assert;
import org.junit.Test;

import org.apache.apex.malhar.lib.testbench.CountAndLastTupleTestSink;

public class RScriptOperatorRealTest
{

  // R scripts are placed at src/main/resources/r
  RScript oper = new RScript("r/aReal.R", "aReal", "retVal");

  @Test
  public void testReal()
  {

    oper.setup(null);
    oper.beginWindow(0);

    CountAndLastTupleTestSink<Object> hashSink = new CountAndLastTupleTestSink<Object>();
    oper.doubleOutput.setSink(hashSink);

    Map<String, RScript.REXP_TYPE> argTypeMap = new HashMap<String, RScript.REXP_TYPE>();
    argTypeMap.put("num1", RScript.REXP_TYPE.REXP_DOUBLE);
    argTypeMap.put("num2", RScript.REXP_TYPE.REXP_DOUBLE);

    oper.setArgTypeMap(argTypeMap);

    HashMap<String, Object> map = new HashMap<String, Object>();

    map.put("num1", 5.2);
    map.put("num2", 12.4);

    oper.inBindings.process(map);
    Assert.assertEquals("Mismatch in number of elements emitted : ", 1, hashSink.count);
    Assert.assertEquals("Mismatch in number of elements emitted : ", (Double)17.6, (Double)hashSink.tuple);

    map = new HashMap<String, Object>();

    map.put("num1", 10.1);
    map.put("num2", 12.6);
    oper.inBindings.process(map);

    oper.endWindow();
    oper.teardown();

    Assert.assertEquals("Mismatch in number of elements emitted : ", 2, hashSink.count);
    Assert.assertEquals("Mismatch in number of elements emitted : ", (Double)22.7, (Double)hashSink.tuple);
  }
}
