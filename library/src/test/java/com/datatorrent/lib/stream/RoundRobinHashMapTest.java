/**
 * Copyright (c) 2012-2012 Malhar, Inc. All rights reserved.
 */
package com.datatorrent.lib.stream;

import com.datatorrent.lib.stream.RoundRobinHashMap;
import com.datatorrent.lib.util.KeyValPair;
import com.malhartech.engine.TestSink;
import java.util.HashMap;
import junit.framework.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Functional test for {@link com.datatorrent.lib.testbench.RoundRobinHashMap}<p>
 * <br>
 */
public class RoundRobinHashMapTest {

    private static Logger log = LoggerFactory.getLogger(RoundRobinHashMapTest.class);


    /**
     * Test oper pass through. The Object passed is not relevant
     */
    @Test
    public void testNodeProcessing() throws Exception
    {
      RoundRobinHashMap oper = new RoundRobinHashMap();
      TestSink mapSink = new TestSink();

      String[] keys = new String[3];
      keys[0] = "a";
      keys[1] = "b";
      keys[2] = "c";

      oper.setKeys(keys);
      oper.map.setSink(mapSink);
      oper.beginWindow(0);

      HashMap<String,Integer> t1 = new HashMap<String,Integer>();
      t1.put("a", 0); t1.put("b", 1); t1.put("c", 2);
      HashMap<String,Integer> t2 = new HashMap<String,Integer>();
      t2.put("a", 3); t2.put("b", 4); t2.put("c", 5);
      HashMap<String,Integer> t3 = new HashMap<String,Integer>();
      t3.put("a", 6); t3.put("b", 7); t3.put("c", 8);

      HashMap<String,Integer> t4 = new HashMap<String,Integer>();
      t4.put("a", 9); t4.put("b", 10); t4.put("c", 11);

      // Same input object can be used as the oper is just pass through
      int numtuples = 12;
      for (int i = 0; i < numtuples; i++) {
        oper.data.process(i);
      }
      oper.endWindow();

      Assert.assertEquals("number emitted tuples", numtuples/3, mapSink.collectedTuples.size());
      log.debug(mapSink.collectedTuples.toString());
      Assert.assertEquals("tuple 1", t1, mapSink.collectedTuples.get(0));
      Assert.assertEquals("tuple 2", t2, mapSink.collectedTuples.get(1));
      Assert.assertEquals("tuple 3", t3, mapSink.collectedTuples.get(2));
      Assert.assertEquals("tuple 4", t4, mapSink.collectedTuples.get(3));
    }
}
