/**
 * Copyright (c) 2012-2012 Malhar, Inc. All rights reserved.
 */
package com.malhartech.lib.testbench;

import com.malhartech.api.BaseOperator;
import com.malhartech.api.DAG;
import com.malhartech.api.DefaultInputPort;
import com.malhartech.api.Operator;
import com.malhartech.api.Sink;
import com.malhartech.dag.StreamConfiguration;
import com.malhartech.dag.Tuple;
import com.malhartech.dag.WindowGenerator;
import com.malhartech.stram.ManualScheduledExecutorService;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import junit.framework.Assert;
import org.apache.hadoop.conf.Configuration;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Functional test for {@link com.malhartech.lib.testbench.RandomEventGenerator}<p>
 * <br>
 * Tests both string and integer. Sets range to 0 to 999 and generates random numbers. With millions
 * of tuple all the values are covered<br>
 * <br>
 * Benchmark: pushes as many tuples are possible<br>
 * String schema does about 3 Million tuples/sec<br>
 * Integer schema does about 7 Million tuples/sec<br>
 * <br>
 * DRC validation is done<br>
 * <br>
 */
public class RandomEventGeneratorTest
{
  private static Logger LOG = LoggerFactory.getLogger(RandomEventGenerator.class);

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
    HashMap<Object, Object> collectedTuples = new HashMap<Object, Object>();
    //DefaultSerDe serde = new DefaultSerDe();
    int count = 0;
    boolean isstring = false;

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
        if (isstring) {
          collectedTuples.put((String)payload, null);
        }
        else {
          collectedTuples.put((Integer)payload, null);
        }
        count++;
      }
    }
  }

  /**
   * Test node logic emits correct results
   */
  @Test
  public void testNodeProcessing() throws Exception
  {
    testSchemaNodeProcessing(true);
    testSchemaNodeProcessing(false);
  }

    public static class CollectorOperator<T> extends BaseOperator
  {
    public final transient CollectorInputPort<T> sdata = new CollectorInputPort<T>("sdata", this);
    public final transient CollectorInputPort<T> vdata = new CollectorInputPort<T>("vdata", this);
    public final transient CollectorInputPort<T> vlist = new CollectorInputPort<T>("vlist", this);
    public final transient CollectorInputPort<T> kvpair = new CollectorInputPort<T>("kvpair", this);
  }

  public void testSchemaNodeProcessing(boolean isstring) throws Exception
  {
    DAG dag = new DAG();
    RandomEventGenerator node = dag.addOperator("randomgen", RandomEventGenerator.class);

    TestSink lgenSink = new TestSink();
    node.connect(RandomEventGenerator.OPORT_DATA, lgenSink);

    lgenSink.isstring = isstring;

    node.setMinvalue(0);
    node.setMaxvalue(1000);
    node.setTuplesblast(50000000);

    LOG.debug("Processed {} tuples and {} unique strings", lgenSink.count, lgenSink.collectedTuples.size());
  }
}
