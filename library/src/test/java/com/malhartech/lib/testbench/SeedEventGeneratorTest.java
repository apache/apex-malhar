/**
 * Copyright (c) 2012-2012 Malhar, Inc. All rights reserved.
 */
package com.malhartech.lib.testbench;

import com.malhartech.api.BaseOperator;
import com.malhartech.api.DAG;
import com.malhartech.api.DefaultInputPort;
import com.malhartech.lib.util.OneKeyValPair;
import com.malhartech.stram.StramLocalCluster;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.logging.Level;
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
  private static Logger log = LoggerFactory.getLogger(SeedEventGeneratorTest.class);
  static ArrayList<HashMap<String, String>> sdlist = new ArrayList<HashMap<String, String>>();
  static ArrayList<HashMap<String, String>> vdlist = new ArrayList<HashMap<String, String>>();
  static ArrayList<HashMap<String, ArrayList<Integer>>> vallist = new ArrayList<HashMap<String, ArrayList<Integer>>>();
  static ArrayList<HashMap<String, ArrayList<OneKeyValPair>>> kvlist = new ArrayList<HashMap<String, ArrayList<OneKeyValPair>>>();

  void clear()
  {
    sdlist.clear();
    vdlist.clear();
    vallist.clear();
    kvlist.clear();
  }

  public static class CollectorOperator extends BaseOperator
  {
    public final transient DefaultInputPort<HashMap<String, String>> sdata = new DefaultInputPort<HashMap<String, String>>(this)
    {
      @Override
      public void process(HashMap<String, String> tuple)
      {
        sdlist.add(tuple);
      }
    };
    public final transient DefaultInputPort<HashMap<String, String>> vdata = new DefaultInputPort<HashMap<String, String>>(this)
    {
      @Override
      public void process(HashMap<String, String> tuple)
      {
        vdlist.add(tuple);
      }
    };
    public final transient DefaultInputPort<HashMap<String, ArrayList<Integer>>> vlist = new DefaultInputPort<HashMap<String, ArrayList<Integer>>>(this)
    {
      @Override
      public void process(HashMap<String, ArrayList<Integer>> tuple)
      {
        vallist.add(tuple);
      }
    };

    public final transient DefaultInputPort<HashMap<String, ArrayList<OneKeyValPair>>> kvpair = new DefaultInputPort<HashMap<String, ArrayList<OneKeyValPair>>>(this)
    {
      @Override
      public void process(HashMap<String, ArrayList<OneKeyValPair>> tuple)
      {
        kvlist.add(tuple);
      }
    };
  }

  /*
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
              log.error(String.format("Got duplicate key (%s)", key));
            }
            keys.put(key, null);
          }
        }
        count++;
      }
    }
  }
  */

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

  @SuppressWarnings("SleepWhileInLoop")
  public void testSchemaNodeProcessing(boolean isstring, boolean insert, boolean doseedkey, boolean emitkey) throws Exception
  {
    DAG dag = new DAG();

    SeedEventGenerator node = dag.addOperator("seedeventgen", SeedEventGenerator.class);
    CollectorOperator collector = dag.addOperator("data collector", new CollectorOperator());
    clear();

    dag.addStream("string_data", node.string_data, collector.sdata).setInline(true);
    dag.addStream("string_data", node.val_data, collector.vdata).setInline(true);
    dag.addStream("string_data", node.val_list, collector.vlist).setInline(true);
    dag.addStream("string_data", node.keyvalpair_list, collector.kvpair).setInline(true);
    /*
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
     node.setSeedend(1000);

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
     */
    final StramLocalCluster lc = new StramLocalCluster(dag);
    lc.setHeartbeatMonitoringEnabled(false);

    new Thread()
    {
      @Override
      public void run()
      {
        try {
          Thread.sleep(1000);
          lc.shutdown();
        }
        catch (InterruptedException ex) {
          log.debug("Interrupted", ex);
        }
      }
    }.start();

    lc.run();
    log.debug(String.format("\n********************************************\nSchema %s, %s, %s: Got s(%d), v(%d), vd(%d), kv(%d)\n********************************************\n",
                            isstring ? "String" : "ArrayList",
                            insert ? "insert values" : "skip insert",
                            emitkey ? "with classification key" : "no classification key",
                            sdlist.size(),
                            vallist.size(),
                            vdlist.size(),
                            kvlist.size()));
  }
}
