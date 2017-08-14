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
package org.apache.apex.benchmark.stream;

import org.apache.apex.malhar.lib.stream.DevNullCounter;
import org.apache.hadoop.conf.Configuration;

import com.datatorrent.api.Context.PortContext;
import com.datatorrent.api.DAG;
import com.datatorrent.api.DAG.Locality;
import com.datatorrent.api.StreamingApplication;
import com.datatorrent.api.annotation.ApplicationAnnotation;

/**
 *
 * Functional tests for {@link org.apache.apex.malhar.lib.testbench.DevNullCounter}.
 * <p>
 * <br>
 * oper.process is called a billion times<br>
 * With extremely high throughput it does not impact the performance of any other oper
 * <br>
 * Benchmarks:<br>
 * Object payload benchmarked at over 1 Million/sec
 * <br>
 * DRC checks are validated<br>
 *
 * @since 2.0.0
 */
@ApplicationAnnotation(name = "DevNullCounterBenchmark")
public class DevNullCounterBenchmark implements StreamingApplication
{
  private final Locality locality = null;
  public static final int QUEUE_CAPACITY = 16 * 1024;

  /**
   * Tests both string and non string schema
   *
   * @param dag
   * @param conf
   */
  @Override
  public void populateDAG(DAG dag, Configuration conf)
  {
    // RandomEventGenerator rand = dag.addOperator("rand", new RandomEventGenerator());
    // rand.setMinvalue(0);
    // rand.setMaxvalue(999999);
    // rand.setTuplesBlastIntervalMillis(50);
    // dag.getMeta(rand).getMeta(rand.integer_data).getAttributes().put(PortContext.QUEUE_CAPACITY, QUEUE_CAPACITY);
    IntegerOperator intInput = dag.addOperator("intInput", new IntegerOperator());
    DevNullCounter oper = dag.addOperator("oper", new DevNullCounter());
    dag.getMeta(oper).getMeta(oper.data).getAttributes().put(PortContext.QUEUE_CAPACITY, QUEUE_CAPACITY);
    dag.addStream("dev", intInput.integer_data, oper.data).setLocality(locality);

  }

}
