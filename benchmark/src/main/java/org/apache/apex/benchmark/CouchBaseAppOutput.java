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
package org.apache.apex.benchmark;

import org.apache.apex.malhar.lib.testbench.RandomEventGenerator;
import org.apache.hadoop.conf.Configuration;

import com.datatorrent.api.DAG;
import com.datatorrent.api.DAG.Locality;

import com.datatorrent.api.StreamingApplication;
import com.datatorrent.api.annotation.ApplicationAnnotation;

/**
 *
 * Application to benchmark the performance of couchbase output operator.
 * The number of tuples processed per second were around 20,000.
 *
 * @since 2.0.0
 */
@ApplicationAnnotation(name = "CouchBaseAppOutput")
public class CouchBaseAppOutput implements StreamingApplication
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
    CouchBaseOutputOperator couchbaseOutput = dag.addOperator("couchbaseOutput", new CouchBaseOutputOperator());
    //couchbaseOutput.getStore().setBucket("default");
    //couchbaseOutput.getStore().setPassword("");
    dag.addStream("ss", rand.integer_data, couchbaseOutput.input).setLocality(locality);
  }

}
