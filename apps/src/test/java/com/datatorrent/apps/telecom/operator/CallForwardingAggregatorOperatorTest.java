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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import junit.framework.Assert;

import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datatorrent.lib.testbench.CollectorTestSink;

/**
 * <p>CallForwardingAggregatorOperatorTest class.</p>
 *
 * @since 0.9.2
 */
public class CallForwardingAggregatorOperatorTest
{

  private static Logger logger = LoggerFactory.getLogger(CallForwardingAggregatorOperatorTest.class);
  
  @Test
  @SuppressWarnings({ "rawtypes", "unchecked" })
  public void testOperator(){
    CallForwardingAggregatorOperator<String,String> opr = new CallForwardingAggregatorOperator<String,String>();
    opr.setWindowSize(2);
    
    List<String> mergeFieldList = new ArrayList<String>();
    mergeFieldList.add("mergeprop1");
    mergeFieldList.add("mergeprop2");
    opr.setMergeFieldList(mergeFieldList);
    
    Map<String, String> acquirerIdentifier = new HashMap<String,String>();
    acquirerIdentifier.put("acquireProp1", "v1");
    acquirerIdentifier.put("acquireProp2", "v2");
    opr.setAcquirerIdentifier(acquirerIdentifier);
    
    Map<String, String> mergeeIdentifier = new HashMap<String, String>();
    mergeeIdentifier.put("mergeeProp1", "v1");
    mergeeIdentifier.put("mergeeProp2", "v2");
    opr.setMergeeIdentifier(mergeeIdentifier);
    
    List<String> matchFieldList = new ArrayList<String>();
    matchFieldList.add("match1");
    matchFieldList.add("match2");
    opr.setMatchFieldList(matchFieldList);
    
    opr.setup(null);
    CollectorTestSink sortSink = new CollectorTestSink();
    opr.output.setSink(sortSink);
    
    opr.beginWindow(0);
    HashMap<String,String> input = new HashMap<String, String>();
    input.put("mergeprop1", "mergeprop1");
    input.put("mergeeProp1", "v1");
    input.put("mergeeProp2", "v2");
    input.put("match1", "match1");
    input.put("match2", "match2");
    opr.input.process(input);
    opr.endWindow();
    
    opr.beginWindow(1);
    HashMap<String,String> input1 = new HashMap<String, String>();
    input1.put("mergeprop2", "mergeprop2");
    input1.put("mergeeProp1", "v1");
    input1.put("mergeeProp2", "v2");
    input1.put("match1", "match1");
    input1.put("match2", "match2");
    opr.input.process(input1);
    opr.endWindow();
    
    opr.beginWindow(2);
    HashMap<String,String> input2 = new HashMap<String, String>();    
    input2.put("acquireProp1", "v1");
    input2.put("acquireProp2", "v2");
    input2.put("match1", "match1");
    input2.put("match2", "match2");
    opr.input.process(input2);
    opr.endWindow();
    
    Assert.assertEquals("number emitted tuples", 1, sortSink.collectedTuples.size());
    for (Object o : sortSink.collectedTuples) {
      logger.debug(o.toString());
      Assert.assertEquals("{match2=match2, mergeprop2=mergeprop2, match1=match1, acquireProp1=v1, acquireProp2=v2}", o.toString());
    }
  }
}
