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

import com.datatorrent.lib.testbench.RandomEventGenerator;

import com.datatorrent.api.Context.PortContext;
import com.datatorrent.api.DAG;
import com.datatorrent.api.StreamingApplication;
import com.datatorrent.api.annotation.ApplicationAnnotation;

/**
 * Application used to benchmark HIVE Map Insert operator
 * The DAG consists of random Event generator operator that is
 * connected to random map output operator that writes to a file in hdfs.&nbsp;
 * The map output operator is connected to hive Map Insert output operator which writes
 * the contents of hdfs file to hive tables.
 * <p>
 *
 */
@ApplicationAnnotation(name = "HiveMapInsertBenchmarkingApp")
public class HiveMapInsertBenchmarkingApp implements StreamingApplication
{
  @Override
  public void populateDAG(DAG dag, Configuration conf)
  {
    dag.setAttribute(DAG.STREAMING_WINDOW_SIZE_MILLIS, 1000);
    RandomEventGenerator eventGenerator = dag.addOperator("EventGenerator", RandomEventGenerator.class);
    RandomMapOutput mapGenerator = dag.addOperator("MapGenerator", RandomMapOutput.class);
    dag.setAttribute(eventGenerator, PortContext.QUEUE_CAPACITY, 10000);
    dag.setAttribute(mapGenerator, PortContext.QUEUE_CAPACITY, 10000);
    dag.addStream("EventGenerator2Map", eventGenerator.integer_data, mapGenerator.input);
    HiveMapInsertOperator hiveMapInsert = dag.addOperator("HiveMapInsertOperator", new HiveMapInsertOperator());
    dag.addStream("MapGenerator2HiveOutput", mapGenerator.map_data, hiveMapInsert.input);
  }
}
