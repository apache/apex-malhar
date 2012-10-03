/**
 * Copyright (c) 2012-2012 Malhar, Inc.
 * All rights reserved.
 */
package com.malhartech.lib.io;

import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.malhartech.dag.ApplicationFactory;
import com.malhartech.dag.DAG;
import com.malhartech.dag.ModuleConfiguration;

/**
 *
 */
public class HdfsOutputTest implements ApplicationFactory {

  private static Logger LOG = LoggerFactory.getLogger(HdfsOutputTest.class);

  private String filepath = "hdfsoutputtest.txt";
  private long numTuples = 1000000;


  public void testThroughPutNoBuffer()
  {

    long startMillis = System.currentTimeMillis();

    Map<String, String> properties = new HashMap<String, String>();
    properties.put(HdfsOutputModule.KEY_FILEPATH, filepath);
    properties.put(HdfsOutputModule.KEY_APPEND, "false");
    ModuleConfiguration config = new ModuleConfiguration("testNode", properties);
    HdfsOutputModule module = new HdfsOutputModule();
    module.setup(config);

    for (int i=0; i<numTuples; i++) {
      module.process("testdata" + i);
    }

    module.flushBytes();
    module.teardown();

    long endMillis = System.currentTimeMillis();
    StringBuilder sb = new StringBuilder();
    sb.append("time taken: " + (endMillis-startMillis) + "ms");
    sb.append("\ntuples written: " + numTuples);
    LOG.info("summary: {}", sb);

  }

  /**
   * Though it is not an application,
   * we can utilize this to run as client on hadoop cluster
   */
  @Override
  public DAG getApplication(Configuration cfg) {

    this.numTuples = cfg.getLong(this.getClass().getName() + ".numTuples", this.numTuples);
    this.filepath = cfg.get(this.getClass().getName() + ".filepath", this.filepath);

    testThroughPutNoBuffer();

    throw new UnsupportedOperationException("Not an application.");
  }

}
