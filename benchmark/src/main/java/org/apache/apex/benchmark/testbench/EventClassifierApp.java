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
import org.apache.apex.malhar.lib.testbench.EventClassifier;
import org.apache.hadoop.conf.Configuration;

import com.datatorrent.api.Context.PortContext;
import com.datatorrent.api.DAG;
import com.datatorrent.api.DAG.Locality;
import com.datatorrent.api.StreamingApplication;
import com.datatorrent.api.annotation.ApplicationAnnotation;

/**
 * Benchmark App for EventClassifier Operator.
 * This operator is benchmarked to emit 1,089,063 tuples/sec on cluster node.
 *
 * @since 2.0.0
 */
@ApplicationAnnotation(name = "EventClassifierApp")
public class EventClassifierApp implements StreamingApplication
{
  private final Locality locality = null;
  public static final int QUEUE_CAPACITY = 16 * 1024;

  @Override
  public void populateDAG(DAG dag, Configuration conf)
  {
    HashMap<String, Double> keymap = new HashMap<String, Double>();
    keymap.put("a", 1.0);
    keymap.put("b", 4.0);
    keymap.put("c", 5.0);
    HashMap<String, ArrayList<Integer>> wmap = new HashMap<String, ArrayList<Integer>>();
    ArrayList<Integer> list = new ArrayList<Integer>(3);
    list.add(60);
    list.add(10);
    list.add(35);
    wmap.put("ia", list);
    list = new ArrayList<Integer>(3);
    list.add(10);
    list.add(75);
    list.add(15);
    wmap.put("ib", list);
    list = new ArrayList<Integer>(3);
    list.add(20);
    list.add(10);
    list.add(70);
    wmap.put("ic", list);
    list = new ArrayList<Integer>(3);
    list.add(50);
    list.add(15);
    list.add(35);
    wmap.put("id", list);
    HashMapOperator hmapOper = dag.addOperator("hmapOper", new HashMapOperator());
    dag.getMeta(hmapOper).getMeta(hmapOper.hmap_data).getAttributes().put(PortContext.QUEUE_CAPACITY, QUEUE_CAPACITY);
    EventClassifier eventClassifier = dag.addOperator("eventClassifier", new EventClassifier());
    eventClassifier.setKeyMap(keymap);
    eventClassifier.setOperationReplace();
    eventClassifier.setKeyWeights(wmap);
    dag.getMeta(eventClassifier).getMeta(eventClassifier.data).getAttributes()
        .put(PortContext.QUEUE_CAPACITY, QUEUE_CAPACITY);
    dag.addStream("eventtest1", hmapOper.hmap_data, eventClassifier.event).setLocality(locality);
    DevNull<HashMap<String, Double>> dev = dag.addOperator("dev", new DevNull());
    dag.addStream("eventtest2", eventClassifier.data, dev.data).setLocality(locality);

  }

}
