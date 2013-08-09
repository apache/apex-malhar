package com.datatorrent.lib.streamquery;

import java.util.HashMap;

import org.junit.Test;

import com.datatorrent.lib.testbench.CollectorTestSink;

public class SelectTopOperatorTest
{
  @SuppressWarnings({"rawtypes", "unchecked"})
  @Test
  public void testOperator() throws Exception
  {
    SelectTopOperator oper = new SelectTopOperator();
    oper.setTopValue(2);
    CollectorTestSink sink = new CollectorTestSink();
    oper.outport.setSink(sink);
    
    oper.beginWindow(1);
    HashMap<String, Object> tuple = new HashMap<String, Object>();
    tuple.put("a", 0);
    tuple.put("b", 1);
    tuple.put("c", 2);
    oper.inport.process(tuple);
    
    tuple = new HashMap<String, Object>();
    tuple.put("a", 1);
    tuple.put("b", 3);
    tuple.put("c", 4);
    oper.inport.process(tuple);
    
    tuple = new HashMap<String, Object>();
    tuple.put("a", 1);
    tuple.put("b", 5);
    tuple.put("c", 6);
    oper.inport.process(tuple);
    oper.endWindow();
    
    System.out.println(sink.collectedTuples.toString());
  }
}
