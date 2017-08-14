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

import org.apache.apex.malhar.lib.stream.DevNull;
import org.apache.apex.malhar.lib.testbench.RandomEventGenerator;
import org.apache.hadoop.conf.Configuration;

import com.datatorrent.api.DAG;
import com.datatorrent.api.DAG.Locality;
import com.datatorrent.api.StreamingApplication;
import com.datatorrent.api.annotation.ApplicationAnnotation;

/**
 * Benchmark App for RandomEventGenerator Operator.
 * This operator is benchmarked to emit 182K tuples/sec on cluster node.
 *
 * @since 2.0.0
 */
@ApplicationAnnotation(name = "RandomEventGeneratorApp")
public class RandomEventGeneratorApp implements StreamingApplication
{
  private final Locality locality = null;
  public static final int QUEUE_CAPACITY = 16 * 1024;

  @Override
  public void populateDAG(DAG dag, Configuration conf)
  {
    RandomEventGenerator random = dag.addOperator("random", new RandomEventGenerator());
    DevNull<Integer> dev1 = dag.addOperator("dev1", new DevNull());
    DevNull<String> dev2 = dag.addOperator("dev2", new DevNull());
    dag.addStream("random1", random.integer_data, dev1.data).setLocality(locality);
    dag.addStream("random2", random.string_data, dev2.data).setLocality(locality);
  }

}
