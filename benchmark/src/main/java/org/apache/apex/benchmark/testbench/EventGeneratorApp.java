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
package org.apache.apex.benchmark.testbench;

import java.util.HashMap;

import org.apache.apex.malhar.lib.stream.DevNull;
import org.apache.apex.malhar.lib.testbench.EventGenerator;
import org.apache.hadoop.conf.Configuration;

import com.datatorrent.api.Context.PortContext;
import com.datatorrent.api.DAG;
import com.datatorrent.api.DAG.Locality;
import com.datatorrent.api.StreamingApplication;
import com.datatorrent.api.annotation.ApplicationAnnotation;

/**
 * Benchmark App for EventGenerator Operator.
 * This operator is benchmarked to emit 1,500,000 tuples/sec on cluster node.
 *
 * @since 2.0.0
 */
@ApplicationAnnotation(name = "EventGeneratorApp")
public class EventGeneratorApp implements StreamingApplication
{
  private final Locality locality = null;
  public static final int QUEUE_CAPACITY = 16 * 1024;

  @Override
  public void populateDAG(DAG dag, Configuration conf)
  {
    EventGenerator eventGenerator = dag.addOperator("eventGenerator", new EventGenerator());
    dag.getMeta(eventGenerator).getMeta(eventGenerator.count).getAttributes()
        .put(PortContext.QUEUE_CAPACITY, QUEUE_CAPACITY);

    DevNull<String> devString = dag.addOperator("devString", new DevNull());
    DevNull<HashMap<String, Double>> devMap = dag.addOperator("devMap", new DevNull());
    DevNull<HashMap<String, Number>> devInt = dag.addOperator("devInt", new DevNull());

    dag.addStream("EventGenString", eventGenerator.string_data, devString.data).setLocality(locality);
    dag.addStream("EventGenMap", eventGenerator.hash_data, devMap.data).setLocality(locality);
    dag.addStream("EventGenInt", eventGenerator.count, devInt.data).setLocality(locality);

  }

}
