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
package com.datatorrent.benchmark.fs;

import com.datatorrent.lib.testbench.RandomWordGenerator;
import com.datatorrent.api.Context.OperatorContext;
import com.datatorrent.api.Context.PortContext;
import com.datatorrent.api.StreamingApplication;
import com.datatorrent.api.DAG;
import com.datatorrent.api.annotation.ApplicationAnnotation;
import com.datatorrent.lib.counters.BasicCounters;
import org.apache.commons.lang.mutable.MutableLong;


import org.apache.hadoop.conf.Configuration;

/**
 * Application used to benchmark HDFS output operator
 * The DAG consists of random word generator operator that is
 * connected to HDFS output operator that writes to a file on HDFS.<p>
 *
 * @since 0.9.4
 */

@ApplicationAnnotation(name="HDFSOutputOperatorBenchmarkingApp")
public class FSOutputOperatorBenchmark implements StreamingApplication
{
  @Override
  public void populateDAG(DAG dag, Configuration conf)
  {
    String filePath = "HDFSOutputOperatorBenchmarkingApp/"
            + System.currentTimeMillis();

    dag.setAttribute(DAG.STREAMING_WINDOW_SIZE_MILLIS, 1000);

    RandomWordGenerator wordGenerator = dag.addOperator("wordGenerator", RandomWordGenerator.class);

    dag.getOperatorMeta("wordGenerator").getMeta(wordGenerator.output).getAttributes().put(PortContext.QUEUE_CAPACITY, 10000);
    dag.getOperatorMeta("wordGenerator").getAttributes().put(OperatorContext.APPLICATION_WINDOW_COUNT, 1);

    FSByteOutputOperator hdfsOutputOperator = dag.addOperator("hdfsOutputOperator", new FSByteOutputOperator());
    hdfsOutputOperator.setFilePath(filePath);
    dag.getOperatorMeta("hdfsOutputOperator").getAttributes().put(OperatorContext.COUNTERS_AGGREGATOR, new BasicCounters.LongAggregator<MutableLong>());

    dag.addStream("Generator2HDFSOutput", wordGenerator.output, hdfsOutputOperator.input);
  }
}


