/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.apex.benchmark.memsql;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.apex.malhar.contrib.memsql.MemsqlPOJOOutputOperator;
import org.apache.apex.malhar.lib.testbench.RandomEventGenerator;
import org.apache.hadoop.conf.Configuration;

import com.datatorrent.api.DAG;
import com.datatorrent.api.DAG.Locality;
import com.datatorrent.api.StreamingApplication;
import com.datatorrent.api.annotation.ApplicationAnnotation;

/**
 * BenchMark Results
 * -----------------
 * The operator operates at 5,500 tuples/sec with the following configuration
 *
 * Container memory size=1G
 * Default memsql configuration
 * memsql number of write threads=1
 * batch size 1000
 * 1 master aggregator, 1 leaf
 *
 * @since 1.0.5
 */
@ApplicationAnnotation(name = "MemsqlOutputBenchmark")
public class MemsqlOutputBenchmark implements StreamingApplication
{
  private static final transient Logger LOG = LoggerFactory.getLogger(MemsqlOutputBenchmark.class);

  public static final int DEFAULT_BATCH_SIZE = 1000;
  public static final int MAX_WINDOW_COUNT = 10000;
  public static final int TUPLE_BLAST_MILLIS = 75;
  public static final int TUPLE_BLAST = 1000;
  public String host = null;

  private static final Locality LOCALITY = null;

  public static class CustomRandomEventGenerator extends RandomEventGenerator
  {
    private boolean done = false;

    @Override
    public void emitTuples()
    {
      if (done) {
        return;
      }

      super.emitTuples();
    }

    @Override
    public void endWindow()
    {
      try {
        super.endWindow();
      } catch (Exception e) {
        done = true;
      }
    }
  }

  @Override
  public void populateDAG(DAG dag, Configuration conf)
  {
    CustomRandomEventGenerator randomEventGenerator = dag.addOperator(
        "randomEventGenerator", new CustomRandomEventGenerator());
    randomEventGenerator.setMaxCountOfWindows(MAX_WINDOW_COUNT);
    randomEventGenerator.setTuplesBlastIntervalMillis(TUPLE_BLAST_MILLIS);
    randomEventGenerator.setTuplesBlast(TUPLE_BLAST);

    LOG.debug("Before making output operator");
    MemsqlPOJOOutputOperator memsqlOutputOperator = dag.addOperator("memsqlOutputOperator",
        new MemsqlPOJOOutputOperator());
    LOG.debug("After making output operator");

    memsqlOutputOperator.setBatchSize(DEFAULT_BATCH_SIZE);

    dag.addStream("memsqlConnector",
        randomEventGenerator.integer_data,
        memsqlOutputOperator.input);
  }
}
