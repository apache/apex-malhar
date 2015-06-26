/**
 * Copyright (C) 2015 DataTorrent, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.datatorrent.benchmark.testbench;

import com.datatorrent.api.Context.PortContext;
import com.datatorrent.api.DAG;
import com.datatorrent.api.DAG.Locality;
import com.datatorrent.api.StreamingApplication;
import com.datatorrent.api.annotation.ApplicationAnnotation;
import com.datatorrent.lib.stream.DevNull;
import com.datatorrent.lib.testbench.SeedEventGenerator;
import com.datatorrent.lib.util.KeyValPair;
import java.util.ArrayList;
import java.util.HashMap;
import org.apache.hadoop.conf.Configuration;

/**
 * Benchmark App for SeedEventGenerator Operator.
 * This operator is benchmarked to emit 800K tuples/sec on cluster node.
 *
 * @since 2.0.0
 */
@ApplicationAnnotation(name = "SeedEventGeneratorApp")
public class SeedEventGeneratorApp implements StreamingApplication
{
  public static final int QUEUE_CAPACITY = 16 * 1024;
  private final Locality locality = null;

  @Override
  public void populateDAG(DAG dag, Configuration conf)
  {
    SeedEventGenerator seedEvent = dag.addOperator("seedEvent", new SeedEventGenerator());
    seedEvent.addKeyData("x", 0, 9);
    seedEvent.addKeyData("y", 0, 9);
    seedEvent.addKeyData("gender", 0, 1);
    seedEvent.addKeyData("age", 10, 19);
    DevNull<HashMap<String, String>> devString = dag.addOperator("devString", new DevNull<HashMap<String, String>>());
    DevNull<HashMap<String, ArrayList<KeyValPair>>> devKeyVal = dag.addOperator("devKeyVal", new DevNull());
    DevNull<HashMap<String, String>> devVal = dag.addOperator("devVal", new DevNull<HashMap<String, String>>());
    DevNull<HashMap<String, ArrayList<Integer>>> devList = dag.addOperator("devList", new DevNull());

    dag.getMeta(seedEvent).getMeta(seedEvent.string_data).getAttributes().put(PortContext.QUEUE_CAPACITY, QUEUE_CAPACITY);
    dag.addStream("SeedEventGeneratorString", seedEvent.string_data, devString.data).setLocality(locality);

    dag.getMeta(seedEvent).getMeta(seedEvent.keyvalpair_list).getAttributes().put(PortContext.QUEUE_CAPACITY, QUEUE_CAPACITY);
    dag.addStream("SeedEventGeneratorKeyVal", seedEvent.keyvalpair_list, devKeyVal.data).setLocality(locality);

    dag.getMeta(seedEvent).getMeta(seedEvent.val_data).getAttributes().put(PortContext.QUEUE_CAPACITY, QUEUE_CAPACITY);
    dag.addStream("SeedEventGeneratorVal", seedEvent.val_data, devVal.data).setLocality(locality);

    dag.getMeta(seedEvent).getMeta(seedEvent.val_list).getAttributes().put(PortContext.QUEUE_CAPACITY, QUEUE_CAPACITY);
    dag.addStream("SeedEventGeneratorValList", seedEvent.val_list, devList.data).setLocality(locality);

  }

}
