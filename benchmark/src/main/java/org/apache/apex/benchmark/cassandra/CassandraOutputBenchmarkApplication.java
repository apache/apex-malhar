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
package org.apache.apex.benchmark.cassandra;

import org.apache.apex.malhar.contrib.cassandra.CassandraTransactionalStore;
import org.apache.apex.malhar.lib.testbench.RandomEventGenerator;
import org.apache.hadoop.conf.Configuration;

import com.datatorrent.api.DAG;
import com.datatorrent.api.DAG.Locality;
import com.datatorrent.api.StreamingApplication;

import com.datatorrent.api.annotation.ApplicationAnnotation;

/**
 * Application to benchmark the performance of cassandra output operator.
 * The operator was tested on following configuration:
 * Virtual Box with 10GB ram, 4 processor cores on an i7 machine with 16GB ram
 * The number of tuples processed per second were around 20,000
 *
 * @since 1.0.3
 */

@ApplicationAnnotation(name = "CassandraOperatorDemo")
public class CassandraOutputBenchmarkApplication implements StreamingApplication
{
  private final Locality locality = null;

  @Override
  public void populateDAG(DAG dag, Configuration conf)
  {
    int maxValue = 1000;

    RandomEventGenerator rand = dag.addOperator("rand", new RandomEventGenerator());
    rand.setMinvalue(0);
    rand.setMaxvalue(maxValue);
    rand.setTuplesBlast(200);

    CassandraOutputOperator cassandra = dag.addOperator("cassandra", new CassandraOutputOperator());
    CassandraTransactionalStore store = new CassandraTransactionalStore();
    store.setKeyspace("test");
    store.setNode("127.0.0.1");
    cassandra.setStore(store);

    dag.addStream("rand_cassandra", rand.integer_data, cassandra.input).setLocality(locality);
  }

}
