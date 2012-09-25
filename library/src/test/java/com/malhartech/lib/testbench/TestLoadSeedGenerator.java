/**
 * Copyright (c) 2012-2012 Malhar, Inc. All rights reserved.
 */
package com.malhartech.lib.testbench;

import com.malhartech.dag.Component;
import com.malhartech.dag.ModuleConfiguration;
import com.malhartech.dag.ModuleContext;
import com.malhartech.dag.Sink;
import com.malhartech.dag.Tuple;
import com.malhartech.stram.ManualScheduledExecutorService;
import com.malhartech.stram.WindowGenerator;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import junit.framework.Assert;
import org.apache.hadoop.conf.Configuration;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * Functional test for {@link com.malhartech.lib.testbench.LoadSeedGenerator}<p>
 * <br>
 * Four keys are sent in at a high throughput rate and the classification is expected to be cover all combinations<br>
 * <br>
 * Benchmarks: A total of 40 million tuples are pushed in each benchmark<br>
 * String schema does about 1.5 Million tuples/sec<br>
 * LoadSeedGenerator.valueData schema is about 4 Million tuples/sec<br>
 * <br>
 * DRC checks are validated<br>
 */
public class TestLoadSeedGenerator
{
  private static Logger LOG = LoggerFactory.getLogger(LoadSeedGenerator.class);

  class TestSink implements Sink
  {
    HashMap<String, Object> keys = new HashMap<String, Object>();
    HashMap<String, Object> ckeys = new HashMap<String, Object>();
    int count = 0;
    boolean isstring = true;
    boolean insert = false;
    int numwindows = 0;

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
                ArrayList alist = (ArrayList) e.getValue();
                for (Object o : alist) {
                  LoadSeedGenerator.valueData vdata = (LoadSeedGenerator.valueData)o;
                  cval += ';' + vdata.str + ":" + vdata.value.toString();
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

      ModuleConfiguration conf = new ModuleConfiguration("mynode", new HashMap<String, String>());
      LoadSeedGenerator node = new LoadSeedGenerator();

      // conf.set(LoadSeedGenerator.KEY_KEYS, "x:0,100;y:0,100;gender:0,1;age:10,120"); // the good key

      conf.set(LoadSeedGenerator.KEY_SEED_END, "10");
      try {
        node.myValidation(conf);
        Assert.fail("validation error  " + LoadSeedGenerator.KEY_SEED_END);
      }
      catch (IllegalArgumentException e) {
        Assert.assertTrue("validate " + LoadSeedGenerator.KEY_SEED_END,
                          e.getMessage().contains("seedstart is empty, but seedend"));
      }

      conf.set(LoadSeedGenerator.KEY_SEED_START, "10");
      conf.set(LoadSeedGenerator.KEY_SEED_END, "");
      try {
        node.myValidation(conf);
        Assert.fail("validation error  " + LoadSeedGenerator.KEY_SEED_START);
      }
      catch (IllegalArgumentException e) {
        Assert.assertTrue("validate " + LoadSeedGenerator.KEY_SEED_START,
                          e.getMessage().contains("but seedend is empty"));
      }

      conf.set(LoadSeedGenerator.KEY_SEED_START, "a");
      conf.set(LoadSeedGenerator.KEY_SEED_END, "10");
      try {
        node.myValidation(conf);
        Assert.fail("validation error  " + LoadSeedGenerator.KEY_SEED_START);
      }
      catch (IllegalArgumentException e) {
        Assert.assertTrue("validate " + LoadSeedGenerator.KEY_SEED_START,
                          e.getMessage().contains("should be an integer"));
      }

      conf.set(LoadSeedGenerator.KEY_SEED_START, "10");
      conf.set(LoadSeedGenerator.KEY_SEED_END, "a");
      try {
        node.myValidation(conf);
        Assert.fail("validation error  " + LoadSeedGenerator.KEY_SEED_END);
      }
      catch (IllegalArgumentException e) {
        Assert.assertTrue("validate " + LoadSeedGenerator.KEY_SEED_END,
                          e.getMessage().contains("should be an integer"));
      }

      conf.set(LoadSeedGenerator.KEY_SEED_START, "0");
      conf.set(LoadSeedGenerator.KEY_SEED_END, "999");
      conf.set(LoadSeedGenerator.KEY_KEYS, "x:0,100;;gender:0,1;age:10,120");
      try {
        node.myValidation(conf);
        Assert.fail("validation error  " + LoadSeedGenerator.KEY_KEYS);
      }
      catch (IllegalArgumentException e) {
        Assert.assertTrue("validate " + LoadSeedGenerator.KEY_KEYS,
                          e.getMessage().contains("slot of parameter \"key\" is empty"));
      }

      conf.set(LoadSeedGenerator.KEY_KEYS, "x:0,100;y:0:100;gender:0,1;age:10,120");
      try {
        node.myValidation(conf);
        Assert.fail("validation error  " + LoadSeedGenerator.KEY_KEYS);
      }
      catch (IllegalArgumentException e) {
        Assert.assertTrue("validate " + LoadSeedGenerator.KEY_KEYS,
                          e.getMessage().contains("malformed in parameter \"key\""));
      }

      conf.set(LoadSeedGenerator.KEY_KEYS, "x:0,100;y:0,100,3;gender:0,1;age:10,120");
      try {
        node.myValidation(conf);
        Assert.fail("validation error  " + LoadSeedGenerator.KEY_KEYS);
      }
      catch (IllegalArgumentException e) {
        Assert.assertTrue("validate " + LoadSeedGenerator.KEY_KEYS,
                          e.getMessage().contains("of parameter \"key\" is malformed"));
      }

      conf.set(LoadSeedGenerator.KEY_KEYS, "x:0,100;y:100,0;gender:0,1;age:10,120");
      try {
        node.myValidation(conf);
        Assert.fail("validation error  " + LoadSeedGenerator.KEY_KEYS);
      }
      catch (IllegalArgumentException e) {
        Assert.assertTrue("validate " + LoadSeedGenerator.KEY_KEYS,
                          e.getMessage().contains("Low value \"100\" is >= high value \"0\" for \"y\""));
      }
      conf.set(LoadSeedGenerator.KEY_KEYS, "x:0,100;y:0,100;gender:0,1;age:10,120");
    }

    /**
     * Test node logic emits correct results
     */
    @Test
    public void testNodeProcessing() throws Exception
    {
      testSchemaNodeProcessing(true, false, false);
      testSchemaNodeProcessing(true, true, false);
      testSchemaNodeProcessing(false, false, false);
      testSchemaNodeProcessing(false, true, false);
      testSchemaNodeProcessing(true, false, true);
      testSchemaNodeProcessing(true, true, true);
      testSchemaNodeProcessing(false, false, true);
      testSchemaNodeProcessing(false, true, true);
    }

    public void testSchemaNodeProcessing(boolean isstring, boolean insert, boolean nokey) throws Exception
    {

      final LoadSeedGenerator node = new LoadSeedGenerator();
      final ManualScheduledExecutorService mses = new ManualScheduledExecutorService(1);
      final WindowGenerator wingen = new WindowGenerator(mses);

      Configuration config = new Configuration();
      config.setLong(WindowGenerator.FIRST_WINDOW_MILLIS, 0);
      config.setInt(WindowGenerator.WINDOW_WIDTH_MILLIS, 1);
      wingen.setup(config);

      Sink input = node.connect(Component.INPUT, wingen);
      wingen.connect("mytestnode", input);


      TestSink seedSink = new TestSink();
      node.connect(LoadSeedGenerator.OPORT_DATA, seedSink);

      ModuleConfiguration conf = new ModuleConfiguration("mynode", new HashMap<String, String>());

      conf.set(LoadSeedGenerator.KEY_SEED_START, "1");
      conf.set(LoadSeedGenerator.KEY_SEED_END, "1000000");
      if (!nokey) {
        conf.set(LoadSeedGenerator.KEY_KEYS, "x:0,9;y:0,9;gender:0,1;age:10,19"); // the good key
      }
      conf.set(LoadSeedGenerator.KEY_STRING_SCHEMA, isstring ? "true" : "false");

      seedSink.isstring = isstring;
      seedSink.insert = insert;

      conf.setInt("SpinMillis", 10);
      conf.setInt("BufferCapacity", 1024 * 1024);
      node.setup(conf);

      final AtomicBoolean inactive = new AtomicBoolean(true);
      new Thread()
      {
        @Override
        public void run()
        {
          inactive.set(false);
          node.activate(new ModuleContext("LoadSeedGeneratorTestNode", this));
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

      int maxticks = insert ? 500 : 200;

      for (int i = 0; i < maxticks; i++) {
        mses.tick(1);
        try {
          Thread.sleep(1);
        }
        catch (InterruptedException e) {
          LOG.error("Unexpected error while sleeping for 1 s", e);
        }
      }
      node.deactivate();

      // Let the reciever get the tuples from the queue
      for (int i = 0; i < 15; i++) {
        try {
          Thread.sleep(1);
        }
        catch (InterruptedException e) {
          LOG.error("Unexpected error while sleeping for 1 s", e);
        }
      }
      LOG.debug(String.format("\n********************************************\nSchema %s, %s, %s: Emitted %d tuples, with %d keys, and %d ckeys\n********************************************\n",
                              isstring ? "String" : "ArrayList",
                              insert ? "insert values" : "skip insert",
                              nokey ? "no classification key" : "with classification key",
                              seedSink.count,
                              seedSink.keys.size(),
                              seedSink.ckeys.size()));
    }
  }
