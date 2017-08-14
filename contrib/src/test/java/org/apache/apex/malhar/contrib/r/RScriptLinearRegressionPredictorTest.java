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

public class RScriptLinearRegressionPredictorTest
{

  // R scripts are placed at src/main/resources/r
  RScript oper = new RScript("r/model.R", "model", "retVal");

  @Test
  public void testLinearRegressionPrediction()
  {

    oper.setup(null);
    oper.beginWindow(0);

    CountAndLastTupleTestSink<Object> hashSink = new CountAndLastTupleTestSink<Object>();
    oper.doubleOutput.setSink(hashSink);

    Map<String, RScript.REXP_TYPE> argTypeMap = new HashMap<String, RScript.REXP_TYPE>();

    argTypeMap.put("PERCENT", RScript.REXP_TYPE.REXP_INT);
    argTypeMap.put("YEAR", RScript.REXP_TYPE.REXP_ARRAY_INT);
    argTypeMap.put("ROLL", RScript.REXP_TYPE.REXP_ARRAY_INT);
    argTypeMap.put("UNEM", RScript.REXP_TYPE.REXP_ARRAY_DOUBLE);
    argTypeMap.put("HGRAD", RScript.REXP_TYPE.REXP_ARRAY_INT);
    argTypeMap.put("INC", RScript.REXP_TYPE.REXP_ARRAY_INT);

    oper.setArgTypeMap(argTypeMap);

    HashMap<String, Object> map = new HashMap<String, Object>();

    int percent = 10;
    int[] year = new int[] {1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29};
    int[] roll = new int[] {5501, 5945, 6629, 7556, 8716, 9369, 9920, 10167, 11084, 12504, 13746, 13656, 13850, 14145, 14888, 14991, 14836, 14478, 14539, 14395, 14599, 14969, 15107, 14831, 15081, 15127, 15856, 15938, 16081 };
    double[] unem = new double[] {8.1, 7.0, 7.3, 7.5, 7.0, 6.4, 6.5, 6.4, 6.3, 7.7, 8.2, 7.5, 7.4, 8.2, 10.1, 9.2, 7.7, 5.7, 6.5, 7.5, 7.3, 9.2, 10.1, 7.5, 8.8, 9.1, 8.8, 7.8, 7.0 };
    int[] hgrad = new int[] {9552, 9680, 9731, 11666, 14675, 15265, 15484, 15723, 16501, 16890, 17203, 17707, 18108, 18266, 19308, 18224, 18997, 19505, 1900, 19546, 19117, 18774, 17813, 17304, 16756, 16749, 16925, 17231, 16816 };
    int[] inc = new int[] {1923, 1961, 1979, 2030, 2112, 2192, 2235, 2351, 2411, 2475, 2524, 2674, 2833, 2863, 2839, 2898, 3123, 3195, 3239, 3129, 3100, 3008, 2983, 3069, 3151, 3127, 3179, 3207, 3345 };

    map.put("PERCENT", percent);
    map.put("YEAR", year);
    map.put("ROLL", roll);
    map.put("UNEM", unem);
    map.put("HGRAD", hgrad);
    map.put("INC", inc);

    oper.inBindings.process(map);
    oper.endWindow();
    oper.teardown();

    Assert.assertEquals("Number of real values received : ", 1, hashSink.count);
  }
}
