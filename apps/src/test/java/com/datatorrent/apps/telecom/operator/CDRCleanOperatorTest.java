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
package com.datatorrent.apps.telecom.operator;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import static junit.framework.Assert.*;

import org.junit.Test;

import com.datatorrent.apps.telecom.operator.CDRCleanOperator.CDRValidator;
import com.datatorrent.lib.testbench.CollectorTestSink;
import com.google.common.collect.Sets;

public class CDRCleanOperatorTest
{
  
  private static final String[] keys = {"Call Type", "Sell Price", "Phone Number"};
  
  private static final Object[][] values = {{"V", 132.4, "2039827836"},{"Z", 34.2, "6378948847"}, {"I", -10.0, "3787784738"}};

  @Test
  @SuppressWarnings({ "rawtypes", "unchecked" })
  public void testOperator()
  {
    CDRCleanOperator oper = new CDRCleanOperator();

    CollectorTestSink successSink = new CollectorTestSink();
    oper.out.setSink(successSink);
    
    CollectorTestSink failureSink = new CollectorTestSink();
    oper.invalidOutput.setSink(failureSink);
    
    oper.registValidator(new CDRValidator() {
      @Override
      public boolean validateTuple(Map<String, Object> tuple)
      {
        return Sets.newHashSet("V", "VOIP", "D", "C", "N", "I", "U", "B", "X", "M", "G").contains(tuple.get("Call Type"));
      }
    });
    
    oper.registValidator(new CDRValidator() {
      
      @Override
      public boolean validateTuple(Map<String, Object> tuple)
      {
        return (Double)tuple.get("Sell Price") > 0 && (Double)tuple.get("Sell Price") < 1000;
      }
    });
    
    for (Map<String, Object> t : getTestMaps()) {
      oper.in.process(t); 
    }
    
    assertEquals("only one valid test tuple", 1, successSink.collectedTuples.size());
    assertEquals("2 invalid test tuples", 2, failureSink.collectedTuples.size());

  }
  
  List<Map<String, Object>> getTestMaps(){
    List<Map<String, Object>> testMaps = new LinkedList<Map<String,Object>>();
    for (Object[] tupleValue : values) {
      Map<String, Object> testRecord = new HashMap<String, Object>();
      int j = 0;
      for (Object object : tupleValue) {
        testRecord.put(keys[j++], object);
      }
      testMaps.add(testRecord);
    }
    return testMaps;
  }

}
