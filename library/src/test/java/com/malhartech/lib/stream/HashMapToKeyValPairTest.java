/**
 * Copyright (c) 2012-2012 Malhar, Inc. All rights reserved.
 */
package com.malhartech.lib.stream;

import com.malhartech.lib.testbench.CountTestSink;
import java.util.HashMap;
import junit.framework.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Functional test for {@link com.malhartech.lib.testbench.HashMapToKeyValPair}<p>
 * <br>
 */
public class HashMapToKeyValPairTest {

    private static Logger log = LoggerFactory.getLogger(HashMapToKeyValPairTest.class);


    /**
     * Test oper pass through. The Object passed is not relevant
     */
    @Test
    public void testNodeProcessing() throws Exception
    {
      HashMapToKeyValPair oper = new HashMapToKeyValPair();
      CountTestSink keySink = new CountTestSink();
      CountTestSink valSink = new CountTestSink();
      CountTestSink keyvalSink = new CountTestSink();

      oper.key.setSink(keySink);
      oper.val.setSink(valSink);
      oper.keyval.setSink(keyvalSink);

      oper.beginWindow(0);
      HashMap<String,String> input = new HashMap<String,String>();
      input.put("a", "1");
      // Same input object can be used as the oper is just pass through
      int numtuples = 1000;
      for (int i = 0; i < numtuples; i++) {
        oper.data.process(input);
      }

      oper.endWindow();

      Assert.assertEquals("number emitted tuples", numtuples, keySink.count);
      Assert.assertEquals("number emitted tuples", numtuples, valSink.count);
      Assert.assertEquals("number emitted tuples", numtuples, keyvalSink.count);
    }
}
