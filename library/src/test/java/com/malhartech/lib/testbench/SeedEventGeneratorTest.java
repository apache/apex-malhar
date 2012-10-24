/**
 * Copyright (c) 2012-2012 Malhar, Inc. All rights reserved.
 */
package com.malhartech.lib.testbench;


import com.malhartech.api.Component;
import com.malhartech.api.OperatorConfiguration;
import com.malhartech.api.Sink;
import com.malhartech.dag.AsyncInputNode;
import com.malhartech.dag.Node;
import com.malhartech.dag.StreamConfiguration;
import com.malhartech.dag.Tuple;
import com.malhartech.dag.WindowGenerator;
import com.malhartech.stram.ManualScheduledExecutorService;
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
                    cval += ';' + ((Integer) o).toString();
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

  @SuppressWarnings("SleepWhileInLoop")
  public void testSchemaNodeProcessing(boolean isstring, boolean insert, boolean doseedkey, boolean emitkey) throws Exception
  {
    SeedEventGenerator node = new SeedEventGenerator();
    final ManualScheduledExecutorService mses = new ManualScheduledExecutorService(1);
    final WindowGenerator wingen = new WindowGenerator(mses);

    StreamConfiguration config = new StreamConfiguration();
    config.setLong(WindowGenerator.FIRST_WINDOW_MILLIS, 0);
    config.setInt(WindowGenerator.WINDOW_WIDTH_MILLIS, 1);
    wingen.setup(config);

    AsyncInputNode inode = new AsyncInputNode("mytestnode", node);
   Sink input = inode.connect(Node.INPUT, wingen);
    wingen.setSink("mytestnode", input);

    TestSink sdataSink = new TestSink();
    TestSink vdataSink = new TestSink();
    TestSink vlistSink = new TestSink();
    TestSink kvpairSink = new TestSink();

    node.string_data.setSink(sdataSink);
    node.val_data.setSink(vdataSink);
    node.val_list.setSink(vlistSink);
    node.keyvalpair_list.setSink(kvpairSink);

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

    node.setup(new OperatorConfiguration());
    wingen.postActivate(null);

    for (int i = 0; i < numtuples; i++) {
      mses.tick(1);
      try {
        Thread.sleep(1);
      }
      catch (InterruptedException ie) {
      }
    }
    
    try {
      Thread.sleep(5);
    }
    catch (InterruptedException ie) {
    }
    finally {
      mses.tick(1);
    }
    LOG.debug(String.format("\n********************************************\nSchema %s, %s, %s: Emitted %d tuples, with %d keys, and %d ckeys\n********************************************\n",
                            isstring ? "String" : "ArrayList",
                            insert ? "insert values" : "skip insert",
                            emitkey ? "with classification key" : "no classification key",
                            sdataSink.count,
                            sdataSink.keys.size(),
                            sdataSink.ckeys.size()));
  }
}
