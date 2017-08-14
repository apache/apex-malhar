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
package org.apache.apex.benchmark.aerospike;

import org.apache.apex.malhar.contrib.aerospike.AerospikeTransactionalStore;
import org.apache.apex.malhar.lib.testbench.RandomEventGenerator;
import org.apache.hadoop.conf.Configuration;

import com.datatorrent.api.DAG;
import com.datatorrent.api.DAG.Locality;
import com.datatorrent.api.StreamingApplication;
import com.datatorrent.api.annotation.ApplicationAnnotation;

/**
 * Application to benchmark the performance of aerospike output operator.
 * The operator was tested on DT cluster and the number of tuples processed
 * by the operator per second were around 12,000
 *
 * @since 1.0.4
 */

@ApplicationAnnotation(name = "AerospikeOutputOperatorBenchmark")
public class AerospikeOutputBenchmarkApplication implements StreamingApplication
{

  private final String NODE = "127.0.0.1";
  private final int PORT = 3000;
  private final String NAMESPACE = "test";
  private final Locality locality = null;

  @Override
  public void populateDAG(DAG dag, Configuration conf)
  {

    RandomEventGenerator rand = dag.addOperator("rand", new RandomEventGenerator());
    rand.setMaxvalue(3000);
    rand.setTuplesBlast(250);

    AerospikeOutputOperator aero = dag.addOperator("aero", new AerospikeOutputOperator());
    AerospikeTransactionalStore store = new AerospikeTransactionalStore();
    store.setNode(NODE);
    store.setPort(PORT);
    store.setNamespace(NAMESPACE);
    aero.setStore(store);

    dag.addStream("rand_aero", rand.integer_data, aero.input).setLocality(locality);
  }

}
