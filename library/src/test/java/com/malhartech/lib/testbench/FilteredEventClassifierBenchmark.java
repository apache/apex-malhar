/**
 * Copyright (c) 2012-2012 Malhar, Inc. All rights reserved.
 */
package com.malhartech.lib.testbench;

import com.esotericsoftware.minlog.Log;
import com.malhartech.api.Sink;
import com.malhartech.dag.Tuple;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Functional test for {@link com.malhartech.lib.testbench.FilteredEventClassifier} for three configuration><p>
 * <br>
 * Configuration 1: Provide values and weights<br>
 * Configuration 2: Provide values but no weights (even weights)<br>
 * Configuration 3: Provide no values or weights<br>
 * <br>
 * Benchmarks: Currently handle about 20 Million tuples/sec incoming tuples in debugging environment. Need to test on larger nodes<br>
 * <br>
 * Validates all DRC checks of the node<br>
 */
public class FilteredEventClassifierBenchmark
{
  private static Logger log = LoggerFactory.getLogger(FilteredEventClassifierBenchmark.class);

  class TestSink implements Sink
  {
    HashMap<String, Integer> collectedTuples = new HashMap<String, Integer>();
    HashMap<String, Double> collectedTupleValues = new HashMap<String, Double>();

    /**
     *
     * @param payload
     */
    @Override
    public void process(Object payload)
    {
      if (payload instanceof Tuple) {
        // log.debug(payload.toString());
      }
      else {
        HashMap<String, Double> tuple = (HashMap<String, Double>)payload;
        for (Map.Entry<String, Double> e: tuple.entrySet()) {
          Integer ival = collectedTuples.get(e.getKey());
          if (ival == null) {
            ival = new Integer(1);
          }
          else {
            ival = ival + 1;
          }
          collectedTuples.put(e.getKey(), ival);
          collectedTupleValues.put(e.getKey(), e.getValue());
        }
      }
    }

    /**
     *
     */
    public void clear()
    {
      collectedTuples.clear();
      collectedTupleValues.clear();
    }
  }

  /**
   * Test node logic emits correct results
   */
  @Test
  @Category(com.malhartech.annotation.PerformanceTestCategory.class)
  public void testNodeProcessing() throws Exception
  {
    FilteredEventClassifier<Double> node = new FilteredEventClassifier<Double>();
    TestSink classifySink = new TestSink();
    node.filter.setSink(classifySink);

    HashMap<String, Double> kmap = new HashMap<String, Double>(3);
    kmap.put("a", 1.0);
    kmap.put("b", 4.0);
    kmap.put("c", 5.0);

    ArrayList<Integer> list = new ArrayList<Integer>(3);
    HashMap<String, ArrayList<Integer>> wmap = new HashMap<String, ArrayList<Integer>>(4);
    list.add(60);
    list.add(10);
    list.add(35);
    wmap.put("ia", list);

    list = new ArrayList<Integer>(3);
    list.add(10);
    list.add(75);
    list.add(15);
    wmap.put("ib", list);

    list = new ArrayList<Integer>(3);
    list.add(20);
    list.add(10);
    list.add(70);
    wmap.put("ic", list);

    list = new ArrayList<Integer>(3);
    list.add(50);
    list.add(15);
    list.add(35);
    wmap.put("id", list);

    node.setKeyMap(kmap);
    node.setKeyWeights(wmap);
    node.setPassFilter(10);
    node.setTotalFilter(100);
    node.setup(new com.malhartech.dag.OperatorContext("irrelevant", null, null));

    int numTuples = 10000000;

    HashMap<String, Double> input = new HashMap<String, Double>();
    int sentval = 0;
    node.beginWindow(0);
    for (int i = 0; i < numTuples; i++) {
      input.clear();
      input.put("a,ia", 2.0);
      input.put("a,ib", 2.0);
      input.put("a,ic", 2.0);
      input.put("a,id", 2.0);
      input.put("b,ia", 2.0);
      input.put("b,ib", 2.0);
      input.put("b,ic", 2.0);
      input.put("b,id", 2.0);
      input.put("c,ia", 2.0);
      input.put("c,ib", 2.0);
      input.put("c,ic", 2.0);
      input.put("c,id", 2.0);

      sentval += 12;
      node.data.process(input);
    }
    node.endWindow();
    int ival = 0;
    for (Map.Entry<String, Integer> e: classifySink.collectedTuples.entrySet()) {
      ival += e.getValue().intValue();
    }

    log.info(String.format("\n*******************************************************\nFiltered %d out of %d intuples with %d and %d unique keys",
                           ival,
                           sentval,
                           classifySink.collectedTuples.size(),
                           classifySink.collectedTupleValues.size()));
    for (Map.Entry<String, Double> ve: classifySink.collectedTupleValues.entrySet()) {
      Integer ieval = classifySink.collectedTuples.get(ve.getKey()); // ieval should not be null?
      Log.info(String.format("%d tuples of key \"%s\" has value %f", ieval.intValue(), ve.getKey(), ve.getValue()));
    }

    // Now test a node with no weights
    FilteredEventClassifier nwnode = new FilteredEventClassifier();
    classifySink.clear();
    nwnode.filter.setSink(classifySink);
    nwnode.setKeyMap(kmap);
    nwnode.setPassFilter(10);
    nwnode.setTotalFilter(100);
    nwnode.setup(new com.malhartech.dag.OperatorContext("irrelevant", null, null));

    node.beginWindow(0);
    sentval = 0;
    for (int i = 0; i < numTuples; i++) {
      input.clear();
      input.put("a,ia", 2.0);
      input.put("a,ib", 2.0);
      input.put("a,ic", 2.0);
      input.put("a,id", 2.0);
      input.put("b,ia", 2.0);
      input.put("b,ib", 2.0);
      input.put("b,ic", 2.0);
      input.put("b,id", 2.0);
      input.put("c,ia", 2.0);
      input.put("c,ib", 2.0);
      input.put("c,ic", 2.0);
      input.put("c,id", 2.0);

      sentval += 12;
      nwnode.data.process(input);
    }
    nwnode.endWindow();
    ival = 0;
    for (Map.Entry<String, Integer> e: classifySink.collectedTuples.entrySet()) {
      ival += e.getValue().intValue();
    }

    log.info(String.format("\n*******************************************************\nFiltered %d out of %d intuples with %d and %d unique keys",
                           ival,
                           sentval,
                           classifySink.collectedTuples.size(),
                           classifySink.collectedTupleValues.size()));
    for (Map.Entry<String, Double> ve: classifySink.collectedTupleValues.entrySet()) {
      Integer ieval = classifySink.collectedTuples.get(ve.getKey()); // ieval should not be null?
      Log.info(String.format("%d tuples of key \"%s\" has value %f", ieval.intValue(), ve.getKey(), ve.getValue()));
    }

    // Now test a node with no weights and no values
    FilteredEventClassifier nvnode = new FilteredEventClassifier();
    classifySink.clear();
    kmap.put("a", 0.0);
    kmap.put("b", 0.0);
    kmap.put("c", 0.0);
    nvnode.filter.setSink(classifySink);
    nvnode.setKeyMap(kmap);
    nvnode.setPassFilter(10);
    nvnode.setTotalFilter(100);
    nvnode.setup(new com.malhartech.dag.OperatorContext("irrelevant", null, null));


    sentval = 0;
    node.beginWindow(0);
    for (int i = 0; i < numTuples; i++) {
      input.clear();
      input.put("a,ia", 2.0);
      input.put("a,ib", 2.0);
      input.put("a,ic", 2.0);
      input.put("a,id", 2.0);
      input.put("b,ia", 2.0);
      input.put("b,ib", 2.0);
      input.put("b,ic", 2.0);
      input.put("b,id", 2.0);
      input.put("c,ia", 2.0);
      input.put("c,ib", 2.0);
      input.put("c,ic", 2.0);
      input.put("c,id", 2.0);

      sentval += 12;
      nvnode.data.process(input);
    }
    nvnode.endWindow();
    ival = 0;
    for (Map.Entry<String, Integer> e: classifySink.collectedTuples.entrySet()) {
      ival += e.getValue().intValue();
    }
    log.info(String.format("\n*******************************************************\nFiltered %d out of %d intuples with %d and %d unique keys",
                           ival,
                           sentval,
                           classifySink.collectedTuples.size(),
                           classifySink.collectedTupleValues.size()));
    log.debug(String.format("\nBenchmarked %d tuples", numTuples * 36));
  }
}
