/**
 * Copyright (c) 2012-2012 Malhar, Inc. All rights reserved.
 */
package com.malhartech.lib.testbench;

import com.malhartech.dag.*;
import com.malhartech.stram.ManualScheduledExecutorService;
import com.malhartech.dag.WindowGenerator;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import junit.framework.Assert;
import org.apache.hadoop.conf.Configuration;
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
   * Test configuration and parameter validation of the node
   */
  @Test
  public void testNodeValidation()
  {

    OperatorConfiguration conf = new OperatorConfiguration("mynode", new HashMap<String, String>());
    LoadSeedGenerator node = new LoadSeedGenerator();

    // conf.set(SeedEventGenerator.KEY_KEYS, "x:0,100;y:0,100;gender:0,1;age:10,120"); // the good key

    conf.set(SeedEventGenerator.KEY_SEED_END, "10");
    try {
      node.myValidation(conf);
      Assert.fail("validation error  " + SeedEventGenerator.KEY_SEED_END);
    }
    catch (IllegalArgumentException e) {
      Assert.assertTrue("validate " + SeedEventGenerator.KEY_SEED_END,
                        e.getMessage().contains("seedstart is empty, but seedend"));
    }

    conf.set(SeedEventGenerator.KEY_SEED_START, "10");
    conf.set(SeedEventGenerator.KEY_SEED_END, "");
    try {
      node.myValidation(conf);
      Assert.fail("validation error  " + SeedEventGenerator.KEY_SEED_START);
    }
    catch (IllegalArgumentException e) {
      Assert.assertTrue("validate " + SeedEventGenerator.KEY_SEED_START,
                        e.getMessage().contains("but seedend is empty"));
    }

    conf.set(SeedEventGenerator.KEY_SEED_START, "a");
    conf.set(SeedEventGenerator.KEY_SEED_END, "10");
    try {
      node.myValidation(conf);
      Assert.fail("validation error  " + SeedEventGenerator.KEY_SEED_START);
    }
    catch (IllegalArgumentException e) {
      Assert.assertTrue("validate " + SeedEventGenerator.KEY_SEED_START,
                        e.getMessage().contains("should be an integer"));
    }

    conf.set(SeedEventGenerator.KEY_SEED_START, "10");
    conf.set(SeedEventGenerator.KEY_SEED_END, "a");
    try {
      node.myValidation(conf);
      Assert.fail("validation error  " + SeedEventGenerator.KEY_SEED_END);
    }
    catch (IllegalArgumentException e) {
      Assert.assertTrue("validate " + SeedEventGenerator.KEY_SEED_END,
                        e.getMessage().contains("should be an integer"));
    }

    conf.set(SeedEventGenerator.KEY_SEED_START, "0");
    conf.set(SeedEventGenerator.KEY_SEED_END, "999");
    conf.set(SeedEventGenerator.KEY_KEYS, "x:0,100;;gender:0,1;age:10,120");
    try {
      node.myValidation(conf);
      Assert.fail("validation error  " + SeedEventGenerator.KEY_KEYS);
    }
    catch (IllegalArgumentException e) {
      Assert.assertTrue("validate " + SeedEventGenerator.KEY_KEYS,
                        e.getMessage().contains("slot of parameter \"key\" is empty"));
    }

    conf.set(SeedEventGenerator.KEY_KEYS, "x:0,100;y:0:100;gender:0,1;age:10,120");
    try {
      node.myValidation(conf);
      Assert.fail("validation error  " + SeedEventGenerator.KEY_KEYS);
    }
    catch (IllegalArgumentException e) {
      Assert.assertTrue("validate " + SeedEventGenerator.KEY_KEYS,
                        e.getMessage().contains("malformed in parameter \"key\""));
    }

    conf.set(SeedEventGenerator.KEY_KEYS, "x:0,100;y:0,100,3;gender:0,1;age:10,120");
    try {
      node.myValidation(conf);
      Assert.fail("validation error  " + SeedEventGenerator.KEY_KEYS);
    }
    catch (IllegalArgumentException e) {
      Assert.assertTrue("validate " + SeedEventGenerator.KEY_KEYS,
                        e.getMessage().contains("of parameter \"key\" is malformed"));
    }

    conf.set(SeedEventGenerator.KEY_KEYS, "x:0,100;y:100,0;gender:0,1;age:10,120");
    try {
      node.myValidation(conf);
      Assert.fail("validation error  " + SeedEventGenerator.KEY_KEYS);
    }
    catch (IllegalArgumentException e) {
      Assert.assertTrue("validate " + SeedEventGenerator.KEY_KEYS,
                        e.getMessage().contains("Low value \"100\" is >= high value \"0\" for \"y\""));
    }
    conf.set(SeedEventGenerator.KEY_KEYS, "x:0,100;y:0,100;gender:0,1;age:10,120");
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

    final SeedEventGenerator node = new SeedEventGenerator();
    final ManualScheduledExecutorService mses = new ManualScheduledExecutorService(1);
    final WindowGenerator wingen = new WindowGenerator(mses);

    Configuration config = new Configuration();
    config.setLong(WindowGenerator.FIRST_WINDOW_MILLIS, 0);
    config.setInt(WindowGenerator.WINDOW_WIDTH_MILLIS, 1);
    wingen.setup(config);

    Sink input = node.connect(Component.INPUT, wingen);
    wingen.connect("mytestnode", input);


    TestSink seedSink = new TestSink();
    node.connect(SeedEventGenerator.OPORT_DATA, seedSink);

    OperatorConfiguration conf = new OperatorConfiguration("mynode", new HashMap<String, String>());

    conf.set(SeedEventGenerator.KEY_SEED_START, "1");
    conf.set(SeedEventGenerator.KEY_SEED_END, "1000000");
    int numtuples = 500;

    if (doseedkey) {
      conf.set(SeedEventGenerator.KEY_KEYS, "x:0,9;y:0,9;gender:0,1;age:10,19"); // the good key
    }
    conf.set(SeedEventGenerator.KEY_STRING_SCHEMA, isstring ? "true" : "false");
    conf.set(SeedEventGenerator.KEY_EMITKEY, emitkey ? "true" : "false");

    seedSink.isstring = isstring;
    seedSink.insert = insert;
    seedSink.emitkey = emitkey;
    if (seedSink.ikeys.isEmpty()) {
      seedSink.ikeys.add("x");
      seedSink.ikeys.add("y");
      seedSink.ikeys.add("genger");
      seedSink.ikeys.add("age");
    }

    conf.setInt("SpinMillis", 10);
    conf.setInt("BufferCapacity", 1024 * 1024);
    node.setup(conf);

    final AtomicBoolean inactive = new AtomicBoolean(true);
    new Thread("SchemaNodeProcessing-" + isstring + ":" + insert + ":" + doseedkey + ":" + emitkey)
    {
      @Override
      public void run()
      {
        inactive.set(false);
        node.activate(new OperatorContext("LoadSeedGeneratorTestNode", this));
        inactive.set(true);
      }
    }.start();

    /**
     * spin while the node gets activated.
     */
    try {
      do {
        Thread.sleep(20);
      }
      while (inactive.get());
    }
    catch (InterruptedException ex) {
      LOG.debug(ex.getLocalizedMessage());
    }

    wingen.activate(null);
    for (int i = 0; i < numtuples; i++) {
      mses.tick(1);
      try {
        Thread.sleep(1);
      }
      catch (InterruptedException ie) {
      }
    }

//      wingen.deactivate();
    //node.deactivate();
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
                            seedSink.count,
                            seedSink.keys.size(),
                            seedSink.ckeys.size()));
  }
}
