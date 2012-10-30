/**
 * Copyright (c) 2012-2012 Malhar, Inc. All rights reserved.
 */
package com.malhartech.lib.testbench;

import com.malhartech.api.BaseOperator;
import com.malhartech.api.DAG;
import com.malhartech.api.DefaultInputPort;
import com.malhartech.api.Operator;
import com.malhartech.api.Sink;
import com.malhartech.api.InputOperatorTest;
import com.malhartech.dag.Tuple;
import com.malhartech.stram.StramLocalCluster;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * Functional test for {@link com.malhartech.lib.testbench.SeedEventGenerator}<p>
 * <br>
 * Four keys are sent in at a high throughput rate and the classification is expected to be cover all combinations<br>
 * <br>
 * Benchmarks: A total of 40 million tuples are pushed in each benchmark<br>
 * String schema does about 1.5 Million tuples/sec<br>
 * SeedEventGenerator.valueData schema is about 4 Million tuples/sec<br>
 * <br>
 * DRC checks are validated<br>
 */
public class SeedEventGeneratorTest
{
  private static Logger LOG = LoggerFactory.getLogger(SeedEventGenerator.class);

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
    HashMap<String, Object> keys = new HashMap<String, Object>();
    HashMap<String, Object> ckeys = new HashMap<String, Object>();
    int count = 0;
    boolean isstring = true;
    boolean insert = false;
    boolean emitkey = false;
    int numwindows = 0;
    ArrayList<String> ikeys = new ArrayList<String>();

    /**
     *
     * @param payload
     */
    @Override
    public void process(Object payload)
    {
      if (payload instanceof Tuple) {
        numwindows++;
      }
      else {
        HashMap<String, Object> tuple = (HashMap<String, Object>)payload;
        if (insert) {
          for (Map.Entry<String, Object> e: tuple.entrySet()) {
            String key = e.getKey();
            Object vobj = e.getValue();
            if (vobj != null) {
              String cval = new String();
              if (isstring) {
                cval = (String)e.getValue();
              }
              else {
                ArrayList alist = (ArrayList)e.getValue();
                int j = 0;
                for (Object o: alist) {
                  if (emitkey) {
                    cval += ";" + ikeys.get(j) + ":" + o.toString();
                    j++;
                  }
                  else {
                    //LoadSeedGenerator.valueData vdata = (SeedEventGenerator.valueData) o;
                    cval += ';' + ((Integer)o).toString();
                  }
                }
              }
              if (ckeys.get(cval) == null) {
                ckeys.put(cval, null);
              }
            }
            Object kval = keys.get(key);
            if (kval != null) {
              LOG.error(String.format("Got duplicate key (%s)", key));
            }
            keys.put(key, null);
          }
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
    testSchemaNodeProcessing(true, false, false, false);
    testSchemaNodeProcessing(true, true, false, false);
    testSchemaNodeProcessing(false, false, false, false);
    testSchemaNodeProcessing(false, true, false, false);
    testSchemaNodeProcessing(true, false, true, false);
    testSchemaNodeProcessing(true, true, true, false);
    testSchemaNodeProcessing(false, false, true, false);
    testSchemaNodeProcessing(false, true, true, false);

    testSchemaNodeProcessing(true, false, false, true);
    testSchemaNodeProcessing(true, true, false, true);
    testSchemaNodeProcessing(false, false, false, true);
    testSchemaNodeProcessing(false, true, false, true);
    testSchemaNodeProcessing(true, false, true, true);
    testSchemaNodeProcessing(true, true, true, true);
    testSchemaNodeProcessing(false, false, true, true);
    testSchemaNodeProcessing(false, true, true, true);
  }

  public static class CollectorOperator<T> extends BaseOperator
  {
    public final transient CollectorInputPort<T> sdata = new CollectorInputPort<T>("sdata", this);
    public final transient CollectorInputPort<T> vdata = new CollectorInputPort<T>("vdata", this);
    public final transient CollectorInputPort<T> vlist = new CollectorInputPort<T>("vlist", this);
    public final transient CollectorInputPort<T> kvpair = new CollectorInputPort<T>("kvpair", this);
  }

  @SuppressWarnings("SleepWhileInLoop")
  public void testSchemaNodeProcessing(boolean isstring, boolean insert, boolean doseedkey, boolean emitkey) throws Exception
  {
    DAG dag = new DAG();

    SeedEventGenerator node = dag.addOperator("seedeventgen", SeedEventGenerator.class);
    CollectorOperator collector = dag.addOperator("data collector", new CollectorOperator<Number>());

    dag.addStream("string_data", node.string_data, collector.sdata).setInline(true);
    dag.addStream("string_data", node.val_data, collector.vdata).setInline(true);
    dag.addStream("string_data", node.val_list, collector.vlist).setInline(true);
    dag.addStream("string_data", node.keyvalpair_list, collector.kvpair).setInline(true);

    TestSink sdataSink = new TestSink();
    TestSink vdataSink = new TestSink();
    TestSink vlistSink = new TestSink();
    TestSink kvpairSink = new TestSink();


    if (doseedkey) {
      node.addKeyData("x", 0, 9);
      node.addKeyData("y", 0, 9);
      node.addKeyData("gender", 0, 1);
      node.addKeyData("age", 10, 19);
    }
    node.setSeedstart(1);
    node.setSeedend(1000000);

    int numtuples = 500;

    sdataSink.isstring = isstring;
    sdataSink.insert = insert;
    sdataSink.emitkey = emitkey;
    if (sdataSink.ikeys.isEmpty()) {
      sdataSink.ikeys.add("x");
      sdataSink.ikeys.add("y");
      sdataSink.ikeys.add("genger");
      sdataSink.ikeys.add("age");
    }

    if (vdataSink.ikeys.isEmpty()) {
      vdataSink.ikeys.add("x");
      vdataSink.ikeys.add("y");
      vdataSink.ikeys.add("genger");
      vdataSink.ikeys.add("age");
    }

    if (vlistSink.ikeys.isEmpty()) {
      vlistSink.ikeys.add("x");
      vlistSink.ikeys.add("y");
      vlistSink.ikeys.add("genger");
      vlistSink.ikeys.add("age");
    }

    if (kvpairSink.ikeys.isEmpty()) {
      kvpairSink.ikeys.add("x");
      kvpairSink.ikeys.add("y");
      kvpairSink.ikeys.add("genger");
      kvpairSink.ikeys.add("age");
    }

    final StramLocalCluster lc = new StramLocalCluster(dag);
    lc.setHeartbeatMonitoringEnabled(false);

    new Thread("LocalClusterController")
    {
      @Override
      public void run()
      {
        try {
          Thread.sleep(1000);
        }
        catch (InterruptedException ex) {
        }
        lc.shutdown();
      }
    }.start();

    lc.run();
    LOG.debug(String.format("\n********************************************\nSchema %s, %s, %s: Emitted %d tuples, with %d keys, and %d ckeys\n********************************************\n",
                            isstring ? "String" : "ArrayList",
                            insert ? "insert values" : "skip insert",
                            emitkey ? "with classification key" : "no classification key",
                            sdataSink.count,
                            sdataSink.keys.size(),
                            sdataSink.ckeys.size()));
  }
}
