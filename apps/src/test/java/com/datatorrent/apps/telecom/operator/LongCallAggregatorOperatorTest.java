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

public class LongCallAggregatorOperatorTest
{

  private static Logger logger = LoggerFactory.getLogger(LongCallAggregatorOperatorTest.class);
  
  @Test
  @SuppressWarnings({ "rawtypes", "unchecked" })
  public void testOperator(){
    LongCallAggregatorOperator opr = new LongCallAggregatorOperator();
    opr.setWindowSize(2);
    
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
    Map<String,String> input = new HashMap<String, String>();
    input.put("mergeprop1", "a");
    input.put("mergeeProp1", "v1");
    input.put("mergeeProp2", "v2");
    input.put("match1", "match1");
    input.put("match2", "match2");
    opr.input.process(input);
    opr.endWindow();
    
    opr.beginWindow(1);
    Map<String,String> input1 = new HashMap<String, String>();
    input1.put("mergeprop2", "b");
    input1.put("mergeeProp1", "v1");
    input1.put("mergeeProp2", "v2");
    input1.put("match1", "match1");
    input1.put("match2", "match2");
    opr.input.process(input1);
    opr.endWindow();
    
    opr.beginWindow(2);
    Map<String,String> input2 = new HashMap<String, String>();    
    input2.put("acquireProp1", "v1");
    input2.put("acquireProp2", "v2");
    input2.put("match1", "match1");
    input2.put("match2", "match2");
    opr.input.process(input2);
    opr.endWindow();
    
    Assert.assertEquals("number emitted tuples", 2, sortSink.collectedTuples.size());
    for (Object o : sortSink.collectedTuples) {
      //System.out.println(o.toString());
      logger.info(o.toString());
    }
  }
}
