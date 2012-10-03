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

  private long numTuples = 1000000;
  private final Map<String, String> moduleConfigProperties = new HashMap<String, String>();

  public void testThroughPut()
  {

    long startMillis = System.currentTimeMillis();

    ModuleConfiguration config = new ModuleConfiguration("testNode", moduleConfigProperties);
    config.setIfUnset(HdfsOutputModule.KEY_FILEPATH, "hdfsoutputtest.txt");
    config.setIfUnset(HdfsOutputModule.KEY_APPEND, "false");

    HdfsOutputModule module = new HdfsOutputModule();
    module.setup(config);

    for (int i=0; i<numTuples; i++) {
      module.process("testdata" + i);
    }

    module.teardown();

    long ellapsedMillis = System.currentTimeMillis() - startMillis;
    StringBuilder sb = new StringBuilder();
    sb.append("\ntime taken: " + ellapsedMillis + "ms");
    sb.append("\ntuples written: " + numTuples);
    sb.append("\nbytes written: " + module.bytesWritten);
    if (ellapsedMillis > 0) {
      sb.append("\nbytes per second: " + (module.bytesWritten *1000L / ellapsedMillis ));
    }
    LOG.info("test summary: {}", sb);

  }

  /**
   * Utilize CLI to run as client on hadoop cluster
   */
  @Override
  public DAG getApplication(Configuration cfg) {

    this.numTuples = cfg.getLong(this.getClass().getName() + ".numTuples", this.numTuples);

    String keyPrefix = this.getClass().getName() + ".module.";
    Map<String, String> values = cfg.getValByRegex(keyPrefix + "*");
    for (Map.Entry<String, String> e : values.entrySet()) {
      moduleConfigProperties.put(e.getKey().replace(keyPrefix, ""), e.getValue());
    }
    LOG.info("module properties: " + moduleConfigProperties);

    testThroughPut();

    throw new UnsupportedOperationException("Not an application.");
  }

}
