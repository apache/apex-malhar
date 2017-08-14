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

import org.apache.apex.benchmark.WordCountOperator;
import org.apache.apex.benchmark.stream.IntegerOperator;
import org.apache.apex.malhar.lib.testbench.EventClassifierNumberToHashDouble;
import org.apache.hadoop.conf.Configuration;

import com.datatorrent.api.Context.PortContext;
import com.datatorrent.api.DAG;
import com.datatorrent.api.DAG.Locality;
import com.datatorrent.api.StreamingApplication;
import com.datatorrent.api.annotation.ApplicationAnnotation;

/**
 * Benchmark App for EventClassifierNumberToHashDouble Operator.
 * This operator is benchmarked to emit 800K tuples/sec on cluster node.
 *
 * @since 2.0.0
 */
@ApplicationAnnotation(name = "EventClassifierNumberToHashDoubleApp")
public class EventClassifierNumberToHashDoubleApp implements StreamingApplication
{
  private final Locality locality = null;
  public static final int QUEUE_CAPACITY = 16 * 1024;

  @Override
  public void populateDAG(DAG dag, Configuration conf)
  {
    WordCountOperator<HashMap<String, Double>> counterString =
        dag.addOperator("counterString", new WordCountOperator<HashMap<String, Double>>());
    dag.getMeta(counterString).getMeta(counterString.input).getAttributes()
        .put(PortContext.QUEUE_CAPACITY, QUEUE_CAPACITY);
    EventClassifierNumberToHashDouble eventClassify =
        dag.addOperator("eventClassify", new EventClassifierNumberToHashDouble());
    dag.getMeta(eventClassify).getMeta(eventClassify.data)
        .getAttributes().put(PortContext.QUEUE_CAPACITY, QUEUE_CAPACITY);
    IntegerOperator intInput = dag.addOperator("intInput", new IntegerOperator());
    dag.addStream("eventclassifier2", intInput.integer_data, eventClassify.event).setLocality(locality);
    dag.addStream("eventclassifier1", eventClassify.data, counterString.input).setLocality(locality);

  }

}
