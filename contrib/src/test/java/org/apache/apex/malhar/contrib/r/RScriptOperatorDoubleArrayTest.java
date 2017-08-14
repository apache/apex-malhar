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

public class RScriptOperatorDoubleArrayTest
{

  // R scripts are placed at src/main/resources/r
  RScript oper = new RScript("r/aDoubleVector.R", "DV", "retVal");

  @Test
  public void testDoubleArray()
  {

    oper.setup(null);
    oper.beginWindow(0);

    CountAndLastTupleTestSink<Object> hashSink = new CountAndLastTupleTestSink<Object>();
    oper.doubleArrayOutput.setSink(hashSink);

    Map<String, RScript.REXP_TYPE> argTypeMap = new HashMap<String, RScript.REXP_TYPE>();

    argTypeMap.put("num1", RScript.REXP_TYPE.REXP_ARRAY_DOUBLE);
    argTypeMap.put("num2", RScript.REXP_TYPE.REXP_ARRAY_DOUBLE);

    oper.setArgTypeMap(argTypeMap);

    HashMap<String, Object> map = new HashMap<String, Object>();

    double[] dArr = new double[5];
    dArr[0] = 0.0;
    dArr[1] = 1.1;
    dArr[2] = 2.2;
    dArr[3] = 3.3;
    dArr[4] = 4.4;

    map.put("num1", dArr);

    double[] dArr1 = new double[5];
    dArr1[0] = 5.5;
    dArr1[1] = 6.6;
    dArr1[2] = 7.7;
    dArr1[3] = 8.8;
    dArr1[4] = 9.9;

    map.put("num2", dArr1);

    oper.inBindings.process(map);
    oper.endWindow();
    oper.teardown();

    Assert.assertEquals("Mismatch in number of integer arrays returned : ", 1, hashSink.count);
    Assert.assertEquals("Mismatch in returned array length: ", 5, ((Double[])hashSink.tuple).length);
  }
}
