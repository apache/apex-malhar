/*
 *  Copyright (c) 2012-2013 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.malhartech.lib.algo;

import com.malhartech.annotation.OutputPortFieldAnnotation;
import com.malhartech.api.DefaultOutputPort;
import com.malhartech.engine.TestSink;
import com.malhartech.lib.util.KeyValPair;
import java.util.ArrayList;
import junit.framework.Assert;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Performance test for {@link com.malhartech.lib.stream.ConsolidatorKeyVal}. <p>
 * Current benchmark 6400 tuples per second. This is since you need to construct object on the output side before sending out.
 * <br>
 *
 * @author Locknath Shil <locknath@malhar-inc.com>
 * <br>
 */
public class KeyValueConsolidatorBenchmark
{
  private static Logger log = LoggerFactory.getLogger(KeyValueConsolidatorBenchmark.class);

  public class MyKeyValueConsolidator extends ConsolidatorKeyVal<String, String, Long>
  {
    /**
     * Output port which consolidate the key value pairs.
     */
    @OutputPortFieldAnnotation(name = "out")
    public final transient DefaultOutputPort<ConsolidatedTuple> out = new DefaultOutputPort<ConsolidatedTuple>(this);

    @Override
    public Object mergeKeyValue(String tuple_key, Object tuple_val, ArrayList list, int port)
    {
      Object obj = list.get(port);

      if (port == 0) {
        String str1 = "";
        if (obj != null) {
          str1 = (String)obj;
        }
        return str1 + tuple_val.toString();
      }
      else if (port == 1) {
        Long val = new Long(0);
        if (obj != null) {
          val = (Long)obj;
        }
        return val.longValue() + (Long)tuple_val;
      }
      else {
        return null;
      }
    }

    @Override
    public void emitConsolidatedTuple(KeyValPair<String, ArrayList<Object>> obj)
    {
      ConsolidatedTuple t = new ConsolidatedTuple();
      t.name = (String)obj.getValue().get(0);
      t.volume = (Long)obj.getValue().get(1);

      //log.debug(String.format("name: %s, volume: %d", t.name, t.volume));
      out.emit(t);
    }

    /**
     * Consolidated tuple generated from all coming inputs.
     */
    public class ConsolidatedTuple
    {
      String name;
      Long volume;

      @Override
      public String toString()
      {
        return name.toString() + volume.toString();
      }
    }
  }

  /**
   * Test
   */
  @Test
  @Category(com.malhartech.annotation.PerformanceTestCategory.class)
  public void testNodeProcessing() throws Exception
  {
    MyKeyValueConsolidator oper = new MyKeyValueConsolidator();
    TestSink sink = new TestSink();
    oper.out.setSink(sink);

    oper.beginWindow(0);
    int numTuples = 10000;
    for (long i = 0; i < numTuples; i++) {
      oper.data1.process(new KeyValPair<String, String>("key1", "a"));
      oper.data2.process(new KeyValPair<String, Long>("key1", i));
      oper.data1.process(new KeyValPair<String, String>("key2", "b"));
      oper.data2.process(new KeyValPair<String, Long>("key2", i + 10));
    }

    oper.endWindow();
    Assert.assertEquals("number emitted tuples", 2, sink.collectedTuples.size());
    log.debug(String.format("\n********************\nProcessed %d tuples\n********************\n", numTuples*4));
  }
}
