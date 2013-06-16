/**
 * Copyright (c) 2012-2012 Malhar, Inc. All rights reserved.
 */
package com.malhartech.lib.logs;

import com.malhartech.lib.testbench.HashTestSink;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import junit.framework.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * Functional tests for {@link com.malhartech.lib.logs.LineTokenizerKeyVal}<p>
 *
 */
public class LineTokenizerKeyValTest
{
  private static Logger log = LoggerFactory.getLogger(LineTokenizerKeyValTest.class);

  /**
   * Test oper logic emits correct results
   */
  @Test
  public void testNodeProcessing()
  {

    LineTokenizerKeyVal oper = new LineTokenizerKeyVal();
    HashTestSink tokenSink = new HashTestSink();

    oper.setSplitBy(",");
    oper.setSplitTokenBy("=");
    oper.tokens.setSink(tokenSink);

    oper.beginWindow(0); //

    String input1 = "a=2,b=3,c=4";
    String input2 = "d=2";
    String input3 = "";
    int numTuples = 1000;
    for (int i = 0; i < numTuples; i++) {
      oper.data.process(input1);
      oper.data.process(input2);
      oper.data.process(input3);
    }
    oper.endWindow(); //
    Assert.assertEquals("number emitted tuples", 2, tokenSink.map.size());
    HashMap<Object,Object> smap = tokenSink.map;
     for (Map.Entry<Object,Object> e: smap.entrySet()) {
       HashMap<String,String> kmap = (HashMap<String,String>) e.getKey();
       for (Map.Entry<String,String> o : kmap.entrySet()) {
         String key = o.getKey();
         String val = o.getValue();
         if (key.equals("a")) {
          Assert.assertEquals("value of \"a\"", "2", val);
        }
        else if (key.equals("b")) {
          Assert.assertEquals("value of \"b\"", "3", val);
        }
        else if (key.equals("c")) {
          Assert.assertEquals("value of \"c\"", "4", val);
        }
        else if (key.equals("d")) {
          Assert.assertEquals("value of \"d\"", "2", val);
        }
       }
     }
  }
}
