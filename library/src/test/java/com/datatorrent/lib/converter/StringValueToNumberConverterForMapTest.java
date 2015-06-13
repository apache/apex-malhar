package com.datatorrent.lib.converter;

import java.io.UnsupportedEncodingException;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import org.junit.Assert;
import org.junit.Test;

import com.datatorrent.lib.testbench.CollectorTestSink;
import com.datatorrent.lib.util.KeyValPair;
import com.datatorrent.lib.util.TestUtils;

public class StringValueToNumberConverterForMapTest {

  @Test
  public void testStringValueToNumericConversion() 
  {
    StringValueToNumberConverterForMap<String> testop = new StringValueToNumberConverterForMap<String>();
    String[] values = {"1.0", "2.0", "3.0"};
    String[] keys = {"a", "b", "c"};
    
    HashMap<String, String> inputMap = new HashMap<String, String>();
    
    for(int i =0 ; i < 3; i++)
    {
      inputMap.put(keys[i], values[i]);      
    }
    
    CollectorTestSink<Map<String, Number>> testsink = new CollectorTestSink<Map<String, Number>>();    
    TestUtils.setSink(testop.output, testsink);
    
    testop.beginWindow(0);
    
    testop.input.put(inputMap);
    
    testop.endWindow();

    Assert.assertEquals(1,testsink.collectedTuples.size());
    
    int cnt = 0;
    
    Map<String, Number> output= testsink.collectedTuples.get(0);
    
    Assert.assertEquals(output.get("a"), 1.0);      
    Assert.assertEquals(output.get("b"), 2.0);      
    Assert.assertEquals(output.get("c"), 3.0);      
    
    
  }
}
