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

import java.util.ArrayList;
import java.util.HashMap;

import org.apache.apex.malhar.lib.stream.DevNull;
import org.apache.apex.malhar.lib.testbench.EventIncrementer;
import org.apache.hadoop.conf.Configuration;

import com.datatorrent.api.DAG;
import com.datatorrent.api.DAG.Locality;
import com.datatorrent.api.StreamingApplication;
import com.datatorrent.api.annotation.ApplicationAnnotation;

/**
 * Benchmark App for EventIncrementer Operator.
 * This operator is benchmarked to emit 700K tuples/second on cluster node.
 *
 * @since 2.0.0
 */
@ApplicationAnnotation(name = "EventIncrementerApp")
public class EventIncrementerApp implements StreamingApplication
{
  private final Locality locality = null;
  public static final int QUEUE_CAPACITY = 16 * 1024;

  @Override
  public void populateDAG(DAG dag, Configuration conf)
  {
    EventIncrementer eventInc = dag.addOperator("eventInc", new EventIncrementer());
    ArrayList<String> keys = new ArrayList<String>(2);
    ArrayList<Double> low = new ArrayList<Double>(2);
    ArrayList<Double> high = new ArrayList<Double>(2);
    keys.add("x");
    keys.add("y");
    low.add(1.0);
    low.add(1.0);
    high.add(100.0);
    high.add(100.0);
    eventInc.setKeylimits(keys, low, high);
    eventInc.setDelta(1);
    HashMapOperator hmapOper = dag.addOperator("hmapOper", new HashMapOperator());
    dag.addStream("eventIncInput1", hmapOper.hmapList_data, eventInc.seed);
    dag.addStream("eventIncInput2", hmapOper.hmapMap_data, eventInc.increment);
    DevNull<HashMap<String, Integer>> dev1 = dag.addOperator("dev1", new DevNull());
    DevNull<HashMap<String, String>> dev2 = dag.addOperator("dev2", new DevNull());
    dag.addStream("eventIncOutput1", eventInc.count, dev1.data).setLocality(locality);
    dag.addStream("eventIncOutput2", eventInc.data, dev2.data).setLocality(locality);

  }

}

