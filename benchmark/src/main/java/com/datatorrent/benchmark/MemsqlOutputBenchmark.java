/*
 * Copyright (c) 2014 DataTorrent, Inc. ALL Rights Reserved.
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

package com.datatorrent.benchmark;

import com.datatorrent.api.DAG;
import com.datatorrent.api.DAG.Locality;
import com.datatorrent.api.DAG.StreamMeta;
import com.datatorrent.api.StreamingApplication;
import com.datatorrent.api.annotation.ApplicationAnnotation;
import com.datatorrent.contrib.memsql.*;
import com.datatorrent.lib.testbench.RandomEventGenerator;
import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
@ApplicationAnnotation(name="MemsqlOutputBenchmark")
public class MemsqlOutputBenchmark implements StreamingApplication
{
  private static transient final Logger LOG = LoggerFactory.getLogger(MemsqlOutputBenchmark.class);

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
      if(done) {
        return;
      }

      super.emitTuples();
    }

    @Override
    public void endWindow()
    {
      try {
        super.endWindow();
      }
      catch(Exception e) {
        done = true;
      }
    }
  }

  @Override
  public void populateDAG(DAG dag, Configuration conf)
  {
    if(host != null) {
      conf.set(this.getClass().getName() + ".host", host);
    }

    AbstractMemsqlOutputOperatorTest.memsqlInitializeDatabase(conf, this.getClass().getName());

    MemsqlStore memsqlStore = AbstractMemsqlOutputOperatorTest.createStore(conf,
                                                                           this.getClass().getName(),
                                                                           true);

    CustomRandomEventGenerator randomEventGenerator = dag.addOperator("randomEventGenerator", new CustomRandomEventGenerator());
    randomEventGenerator.setMaxcountofwindows(MAX_WINDOW_COUNT);
    randomEventGenerator.setTuplesBlastIntervalMillis(TUPLE_BLAST_MILLIS);
    randomEventGenerator.setTuplesBlast(TUPLE_BLAST);

    MemsqlOutputOperator memsqlOutputOperator = dag.addOperator("memsqlOutputOperator",
                                                                new MemsqlOutputOperator());

    memsqlOutputOperator.setStore(memsqlStore);
    memsqlOutputOperator.setBatchSize(DEFAULT_BATCH_SIZE);

    StreamMeta streamMeta = dag.addStream("memsqlConnector",
                                          randomEventGenerator.integer_data,
                                          memsqlOutputOperator.input);
    streamMeta.setLocality(LOCALITY);
  }
}
