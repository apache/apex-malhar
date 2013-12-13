package com.datatorrent.apps.telecom.operator;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import junit.framework.Assert;

import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datatorrent.lib.testbench.CollectorTestSink;

public class EnricherOperatorTest
{
  public static class TestEnricher implements EnricherInterface{

    @Override
    public void configure(Properties prop)
    {
      
    }

    @Override
    public void enrichRecord(Map<String, String> m)
    {
      m.put("prop2", "b");
    }
    
  }

  private static Logger logger = LoggerFactory.getLogger(EnricherOperatorTest.class);
  @Test
  @SuppressWarnings({ "rawtypes", "unchecked" })
  public void testOperator(){
    EnrichmentOperator oper = new EnrichmentOperator();
    oper.setProp(new Properties());
    oper.setEnricher(TestEnricher.class);
    oper.setup(null);
    CollectorTestSink sortSink = new CollectorTestSink();
    oper.output.setSink(sortSink);
    
    Map<String,String> input = new HashMap<String, String>();
    input.put("prop1", "a");
    
    
    oper.beginWindow(0);
    oper.input.process(input);
    oper.endWindow();
    
    Assert.assertEquals("number emitted tuples", 1, sortSink.collectedTuples.size());
    for (Object o : sortSink.collectedTuples) {
      Assert.assertEquals("{prop2=b, prop1=a}", o.toString());
    }
  }
}
