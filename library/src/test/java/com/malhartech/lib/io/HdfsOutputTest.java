/**
 * Copyright (c) 2012-2012 Malhar, Inc.
 * All rights reserved.
 */
package com.malhartech.lib.io;

import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.malhartech.api.DAG;
import com.malhartech.api.ApplicationFactory;
import com.malhartech.stram.DAGPropertiesBuilder;

public class HdfsOutputTest implements ApplicationFactory {

  private static Logger LOG = LoggerFactory.getLogger(HdfsOutputTest.class);
  public static final String KEY_FILEPATH = "filepath";
  public static final String KEY_APPEND = "append";

  private long numTuples = 1000000;
  private final Configuration config = new Configuration(false);

  public void testThroughPut()
  {

    long startMillis = System.currentTimeMillis();

    HdfsOutputOperator<Object> module = new HdfsOutputOperator<Object>();
    module.setFilePath(config.get(KEY_FILEPATH, "hdfsoutputtest.txt"));
    module.setAppend(config.getBoolean(KEY_APPEND, false));

    module.setup(new com.malhartech.engine.OperatorContext("irrelevant", null, null));

    for (int i=0; i<=numTuples; i++) {
      module.input.process("testdata" + i);
    }

    module.teardown();

    long ellapsedMillis = System.currentTimeMillis() - startMillis;
    StringBuilder sb = new StringBuilder();
    sb.append("\ntime taken: " + ellapsedMillis + "ms");
    sb.append("\ntuples written: " + numTuples);
    sb.append("\nbytes written: " + module.getTotalBytesWritten());
    if (ellapsedMillis > 0) {
      sb.append("\nbytes per second: " + (module.getTotalBytesWritten() * 1000L / ellapsedMillis ));
    }
    LOG.info("test summary: {}", sb);

  }

  /**
   * Utilize CLI to run as client on hadoop cluster
   */
  @Override
  public DAG getApplication(Configuration cfg) {

    this.numTuples = cfg.getLong(this.getClass().getName() + ".numTuples", this.numTuples);

    String keyPrefix = this.getClass().getName() + ".operator.";
    Map<String, String> values = cfg.getValByRegex(keyPrefix + "*");
    for (Map.Entry<String, String> e : values.entrySet()) {
      this.config.set(e.getKey().replace(keyPrefix, ""), e.getValue());
    }
    LOG.info("properties: " + DAGPropertiesBuilder.toProperties(config, ""));

    testThroughPut();

    throw new UnsupportedOperationException("Not an application.");
  }

}
