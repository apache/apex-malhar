package com.datatorrent.lib.converter;

import java.util.HashMap;

import org.junit.Assert;
import org.junit.Test;

import com.datatorrent.lib.testbench.CollectorTestSink;
import com.datatorrent.lib.util.KeyValPair;
import com.datatorrent.lib.util.TestUtils;

public class MapToKeyValuePairConverterTest {

  @Test
  public void MapToKeyValuePairConversion() 
  {
    MapToKeyValuePairConverter<String, Integer> testop = new MapToKeyValuePairConverter<String, Integer>();
    Integer[] values = {1, 2, 3};
    String[] keys = {"a", "b", "c"};
    
    HashMap<String, Integer> inputMap = new HashMap<String, Integer>();
    
    for(int i =0 ; i < 3; i++)
    {
      inputMap.put(keys[i], values[i]);      
    }
    
    CollectorTestSink<KeyValPair<String, Integer>> testsink = new CollectorTestSink<KeyValPair<String, Integer>>();    
    TestUtils.setSink(testop.output, testsink);
    
    testop.beginWindow(0);
    
    testop.input.put(inputMap);
    
    testop.endWindow();

    Assert.assertEquals(3,testsink.collectedTuples.size());
  }
}
