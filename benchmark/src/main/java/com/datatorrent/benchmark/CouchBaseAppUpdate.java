/*
 * Copyright (c) 2014 DataTorrent, Inc. ALL Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.datatorrent.benchmark;

import com.datatorrent.api.DAG;
import com.datatorrent.api.StreamingApplication;
import com.datatorrent.lib.testbench.RandomEventGenerator;
import org.apache.hadoop.conf.Configuration;
import com.datatorrent.api.DAG.Locality;
import com.datatorrent.api.annotation.ApplicationAnnotation;

/**
 *
 *Application to benchmark the performance of couchbase update operator.
 *The number of tuples processed per second were around 6000.
 *
 * @since 1.0.3
 */
@ApplicationAnnotation(name = "CouchBaseAppUpdate")
public class CouchBaseAppUpdate implements StreamingApplication
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
    CouchBaseUpdateOperator couchbaseUpdate = dag.addOperator("couchbaseUpdate", new CouchBaseUpdateOperator());
        // couchbaseUpdate.getStore().setBucket("default");
    // couchbaseUpdate.getStore().setPassword("");
    dag.addStream("ss", rand.integer_data, couchbaseUpdate.input).setLocality(locality);
  }

}
