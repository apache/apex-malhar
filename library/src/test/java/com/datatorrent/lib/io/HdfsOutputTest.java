/*
 * Copyright (c) 2013 Malhar Inc. ALL Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.datatorrent.lib.io;

import com.datatorrent.api.AttributeMap;
import com.datatorrent.api.AttributeMap.AttributeKey;
import com.datatorrent.api.Context;
import com.datatorrent.api.Context.OperatorContext;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datatorrent.api.StreamingApplication;
import com.datatorrent.api.DAG;
import com.datatorrent.lib.io.HdfsOutputOperator;
import java.util.Iterator;
import java.util.Map.Entry;
import java.util.Properties;
//import com.datatorrent.stram.DAGPropertiesBuilder;

public class HdfsOutputTest implements StreamingApplication {

  private static Logger LOG = LoggerFactory.getLogger(HdfsOutputTest.class);
  public static final String KEY_FILEPATH = "filepath";
  public static final String KEY_APPEND = "append";

  private long numTuples = 1000000;
  private final Configuration config = new Configuration(false);

  public class TestContext implements Context {

    @Override
    public AttributeMap getAttributes()
    {
      return null;
    }

    @Override
    public <T> T attrValue(AttributeKey<T> key, T defaultValue)
    {
      return defaultValue;
    }

  }

  public class TestOperatorContext extends TestContext implements OperatorContext {

    @Override
    public int getId()
    {
      return 0;
    }

  }

  public void testThroughPut()
  {

    long startMillis = System.currentTimeMillis();

    HdfsOutputOperator module = new HdfsOutputOperator();
    module.setFilePath(config.get(KEY_FILEPATH, "hdfsoutputtest.txt"));
    module.setAppend(config.getBoolean(KEY_APPEND, false));

    module.setup(new TestOperatorContext());

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
  public void populateDAG(DAG dag, Configuration cfg) {

    this.numTuples = cfg.getLong(this.getClass().getName() + ".numTuples", this.numTuples);

    String keyPrefix = this.getClass().getName() + ".operator.";
    Map<String, String> values = cfg.getValByRegex(keyPrefix + "*");
    for (Map.Entry<String, String> e : values.entrySet()) {
      this.config.set(e.getKey().replace(keyPrefix, ""), e.getValue());
    }
    LOG.info("properties: " + getConfProperties());

    testThroughPut();

    throw new UnsupportedOperationException("Not an application.");
  }

  private Properties getConfProperties() {
    Properties props = new Properties();
    Iterator<Entry<String,String>> entries = config.iterator();
    while (entries.hasNext()) {
      Entry<String,String> entry = entries.next();
      props.put(entry.getKey(), entry.getValue());
    }
    return props;
  }

}
