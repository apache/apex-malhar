/**
 * Copyright (c) 2012-2012 Malhar, Inc. All rights reserved.
 */
package com.malhartech.lib.testbench;

import com.malhartech.api.BaseOperator;
import com.malhartech.api.DAG;
import com.malhartech.api.DefaultInputPort;
import com.malhartech.api.Operator;
import com.malhartech.api.OperatorConfiguration;
import com.malhartech.api.Sink;
import com.malhartech.dag.Tuple;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * Functional tests for {@link com.malhartech.lib.testbench.EventGenerator}. <p>
 * <br>
 * Load is generated and the tuples are outputted to ensure that the numbers are roughly in line with the weights<br>
 * <br>
 * Benchmarks:<br>
 * String schema generates over 11 Million tuples/sec<br>
 * HashMap schema generates over 1.7 Million tuples/sec<br>
 * <br>
 * DRC checks are validated<br>
 *
 */
public class EventGeneratorTest
{
  private static Logger LOG = LoggerFactory.getLogger(EventGenerator.class);

  public static class CollectorInputPort<T> extends DefaultInputPort<T>
  {
    ArrayList<T> list;
    final String id;

    public CollectorInputPort(String id, Operator module)
    {
      super(module);
      this.id = id;
    }

    @Override
    public void process(T tuple)
    {
      list.add(tuple);
    }
  }

  class TestSink implements Sink
  {
    HashMap<String, Integer> collectedTuples = new HashMap<String, Integer>();
    //DefaultSerDe serde = new DefaultSerDe();
    int count = 0;
    boolean test_hashmap = false;
    boolean skiphash = true;

    /**
     *
     * @param payload
     */
    @Override
    public void process(Object payload)
    {
      if (payload instanceof Tuple) {
        // LOG.debug(payload.toString());
      }
      else {
        if (!skiphash) {
          if (test_hashmap) {
            HashMap<String, Double> tuple = (HashMap<String, Double>)payload;
            for (Map.Entry<String, Double> e: tuple.entrySet()) {
              String str = e.getKey();
              Integer val = collectedTuples.get(str);
              if (val != null) {
                val = val + 1;
              }
              else {
                val = new Integer(1);
              }
              collectedTuples.put(str, val);
            }
          }
          else {
            String str = (String)payload;
            Integer val = collectedTuples.get(str);
            if (val != null) {
              val = val + 1;
            }
            else {
              val = new Integer(1);
            }
            collectedTuples.put(str, val);
          }
        }
        count++;
        //serde.toByteArray(payload);
      }
    }
  }

  class TestCountSink implements Sink
  {
    //DefaultSerDe serde = new DefaultSerDe();
    int count = 0;
    int average = 0;
    int num_tuples = 0;

    /**
     *
     * @param payload
     */
    @Override
    public void process(Object payload)
    {
      if (payload instanceof Tuple) {
        // LOG.debug(payload.toString());
      }
      else {
        HashMap<String, Integer> tuples = (HashMap<String, Integer>)payload;
        average = tuples.get(EventGenerator.OPORT_COUNT_TUPLE_AVERAGE).intValue();
        count += tuples.get(EventGenerator.OPORT_COUNT_TUPLE_COUNT).intValue();
        num_tuples++;
        //serde.toByteArray(payload);
      }
    }
  }


  /**
   * Tests both string and non string schema
   */
  @Test
  public void testNodeProcessing() throws Exception
  {
    testSingleSchemaNodeProcessing(true, true); // 10.5 million/s
    testSingleSchemaNodeProcessing(true, false); // 7.5 million/s
    testSingleSchemaNodeProcessing(false, true); // 4.7 million/s
    testSingleSchemaNodeProcessing(false, false); // 3 million/s
  }

  /**
   * Test node logic emits correct results
   */

  public static class CollectorOperator extends BaseOperator
  {
    public final transient CollectorInputPort<String> sdata = new CollectorInputPort<String>("sdata", this);
    public final transient CollectorInputPort<HashMap<String, Double>> hdata = new CollectorInputPort<HashMap<String, Double>>("hdata", this);
    public final transient CollectorInputPort<HashMap<String, Number>> count = new CollectorInputPort<HashMap<String, Number>>("count", this);
  }

  public void testSingleSchemaNodeProcessing(boolean stringschema, boolean skiphash) throws Exception
  {
    DAG dag = new DAG();
    EventGenerator node = dag.addOperator("eventgen", EventGenerator.class);
    CollectorOperator collector = dag.addOperator("data collector", new CollectorOperator());

    node.setKeys("a,b,c,d");
    node.setValues("");
    if (stringschema) {
      dag.addStream("stest", node.string_data, collector.sdata).setInline(true);
    }
    else {
      dag.addStream("htest", node.hash_data, collector.hdata).setInline(true);
    }
    dag.addStream("hcest", node.count, collector.count).setInline(true);

    node.setWeights("10,40,20,30");
    node.setTuplesBlast(10000000);
    node.setRollingWindowCount(5);
    node.setup(new OperatorConfiguration());

    // Assert.assertEquals("number emitted tuples", 5000, lgenSink.collectedTuples.size());
//        LOG.debug("Processed {} tuples out of {}", lgenSink.collectedTuples.size(), lgenSink.count);
    /*
    LOG.debug(String.format("\n********************************************\nTesting with %s and%s insertion\nLoadGenerator emitted %d (%d) tuples in %d windows; Sink processed %d tuples",
                            stringschema ? "String" : "HashMap", skiphash ? " no" : "",
                            countSink.count, countSink.average, countSink.num_tuples, lgenSink.count));

    for (Map.Entry<String, Integer> e: lgenSink.collectedTuples.entrySet()) {
      LOG.debug("{} tuples for key {}", e.getValue().intValue(), e.getKey());
    }
*/
  }
}
