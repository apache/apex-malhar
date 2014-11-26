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


import org.apache.hadoop.conf.Configuration;

import com.datatorrent.lib.testbench.RandomWordGenerator;

import com.datatorrent.api.Context.PortContext;
import com.datatorrent.api.DAG;
import com.datatorrent.api.StreamingApplication;
import com.datatorrent.api.annotation.ApplicationAnnotation;

/**
 * Application used to benchmark HIVE Insert operator
 * The DAG consists of random word generator operator that is
 * connected to Hive output operator that writes to a Hive table using file written in hdfs.&nbsp;
 * The file contents are written by the word generator.
 * <p>
 *
 */
@ApplicationAnnotation(name = "HiveInsertBenchmarkingApp")
public class HiveInsertBenchmarkingApp implements StreamingApplication
{
  @Override
  public void populateDAG(DAG dag, Configuration conf)
  {
    dag.setAttribute(DAG.STREAMING_WINDOW_SIZE_MILLIS, 1000);
    RandomWordGenerator wordGenerator = dag.addOperator("WordGenerator", RandomWordGenerator.class);
    dag.setAttribute(wordGenerator, PortContext.QUEUE_CAPACITY, 10000);
    HiveInsertOperator hiveInsert = dag.addOperator("HiveInsertOperator",new HiveInsertOperator());
    dag.addStream("Generator2HDFSOutput", wordGenerator.outputString, hiveInsert.input);
  }

}
