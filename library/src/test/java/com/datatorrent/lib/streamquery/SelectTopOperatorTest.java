package com.datatorrent.lib.streamquery;

import org.junit.Test;

import com.datatorrent.lib.testbench.CollectorTestSink;

public class SelectTopOperatorTest
{
  @SuppressWarnings({"rawtypes", "unchecked"})
  @Test
  public void testOperator() throws Exception
  {
    SelectTopOperator<Integer> oper = new SelectTopOperator<Integer>();
    oper.setTopValue(2);
    CollectorTestSink sink = new CollectorTestSink();
    oper.outport.setSink(sink);
    
    oper.beginWindow(1);
    oper.inport.process(1);
    oper.inport.process(5);
    oper.inport.process(-1);
    oper.inport.process(9);
    oper.endWindow();
    
    System.out.println(sink.collectedTuples.toString());
  }
}
