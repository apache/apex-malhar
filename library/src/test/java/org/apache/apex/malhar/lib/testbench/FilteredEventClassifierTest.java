/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.apex.malhar.lib.testbench;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datatorrent.api.Sink;


/**
 * Functional test for {@link org.apache.apex.malhar.lib.testbench.FilteredEventClassifier} for three configuration><p>
 * <br>
 * Configuration 1: Provide values and weights<br>
 * Configuration 2: Provide values but no weights (even weights)<br>
 * Configuration 3: Provide no values or weights<br>
 * <br>
 * Benchmarks: Currently handle about 20 Million tuples/sec incoming tuples in debugging environment. Need to test on larger nodes<br>
 * <br>
 * Validates all DRC checks of the node<br>
 */
public class FilteredEventClassifierTest
{
  private static Logger LOG = LoggerFactory.getLogger(FilteredEventClassifier.class);

  @SuppressWarnings("rawtypes")
  class TestSink implements Sink
  {
    HashMap<String, Integer> collectedTuples = new HashMap<String, Integer>();
    HashMap<String, Double> collectedTupleValues = new HashMap<String, Double>();

    /**
     *
     * @param payload
     */
    @SuppressWarnings("unchecked")
    @Override
    public void put(Object payload)
    {
      HashMap<String, Double> tuple = (HashMap<String, Double>)payload;
      for (Map.Entry<String, Double> e : tuple.entrySet()) {
        Integer ival = collectedTuples.get(e.getKey());
        if (ival == null) {
          ival = 1;
        } else {
          ival = ival + 1;
        }
        collectedTuples.put(e.getKey(), ival);
        collectedTupleValues.put(e.getKey(), e.getValue());
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

    @Override
    public int getCount(boolean reset)
    {
      throw new UnsupportedOperationException("Not supported yet.");
    }
  }

  /**
   * Test node logic emits correct results
   */
  @SuppressWarnings({ "rawtypes", "unchecked" })
  @Test
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
    node.setup(null);

    int numTuples = 10000;
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

    LOG.info(String.format(
        "\n*******************************************************\nFiltered %d out of %d intuples with %d and %d " + "unique keys",
        ival,
        sentval,
        classifySink.collectedTuples.size(),
        classifySink.collectedTupleValues.size()));
    for (Map.Entry<String, Double> ve : classifySink.collectedTupleValues.entrySet()) {
      Integer ieval = classifySink.collectedTuples.get(ve.getKey()); // ieval should not be null?
      LOG.info(String.format("%d tuples of key \"%s\" has value %f", ieval, ve.getKey(), ve.getValue()));
    }

    // Now test a node with no weights
    FilteredEventClassifier nwnode = new FilteredEventClassifier();
    classifySink.clear();
    nwnode.filter.setSink(classifySink);
    nwnode.setKeyMap(kmap);
    nwnode.setPassFilter(10);
    nwnode.setTotalFilter(100);
    nwnode.setup(null);

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
      nwnode.data.process(input);
    }
    nwnode.endWindow();
    ival = 0;
    for (Map.Entry<String, Integer> e: classifySink.collectedTuples.entrySet()) {
      ival += e.getValue().intValue();
    }

    LOG.info(String.format(
        "\n*******************************************************\nFiltered %d out of %d intuples with %d and %d " + "unique keys",
        ival,
        sentval,
        classifySink.collectedTuples.size(),
        classifySink.collectedTupleValues.size()));
    for (Map.Entry<String, Double> ve : classifySink.collectedTupleValues.entrySet()) {
      Integer ieval = classifySink.collectedTuples.get(ve.getKey()); // ieval should not be null?
      LOG.info(String.format("%d tuples of key \"%s\" has value %f", ieval.intValue(), ve.getKey(), ve.getValue()));
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
    nvnode.setup(null);


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
    for (Map.Entry<String, Integer> e : classifySink.collectedTuples.entrySet()) {
      ival += e.getValue();
    }
    LOG.info(String.format(
        "\n*******************************************************\nFiltered %d out of %d intuples with %d and %d " + "unique keys",
        ival,
        sentval,
        classifySink.collectedTuples.size(),
        classifySink.collectedTupleValues.size()));

    for (Map.Entry<String, Double> ve : classifySink.collectedTupleValues.entrySet()) {
      Integer ieval = classifySink.collectedTuples.get(ve.getKey()); // ieval should not be null?
      LOG.info(String.format("%d tuples of key \"%s\" has value %f",
          ieval,
          ve.getKey(),
          ve.getValue()));
    }
  }
}
