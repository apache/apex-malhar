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
package org.apache.apex.benchmark.spillable;

import org.apache.apex.malhar.lib.fileaccess.TFileImpl;
import org.apache.apex.malhar.lib.state.spillable.managed.ManagedStateSpillableStateStore;
import org.apache.hadoop.conf.Configuration;

import com.google.common.base.Preconditions;

import com.datatorrent.api.DAG;
import com.datatorrent.api.StreamingApplication;
import com.datatorrent.api.annotation.ApplicationAnnotation;

@ApplicationAnnotation(name = "SpillableBenchmarkApp")
/**
 * @since 3.6.0
 */
public class SpillableBenchmarkApp implements StreamingApplication
{
  protected final String PROP_STORE_PATH = "dt.application.SpillableBenchmarkApp.storeBasePath";

  @Override
  public void populateDAG(DAG dag, Configuration conf)
  {
    // Create ActiveMQStringSinglePortOutputOperator
    SpillableTestInputOperator input = new SpillableTestInputOperator();
    input.batchSize = 100;
    input.sleepBetweenBatch = 0;
    input = dag.addOperator("input", input);

    SpillableTestOperator testOperator = new SpillableTestOperator();
    testOperator.store = createStore(conf);
    testOperator.shutdownCount = -1;
    testOperator = dag.addOperator("test", testOperator );


    // Connect ports
    dag.addStream("stream", input.output, testOperator.input).setLocality(DAG.Locality.CONTAINER_LOCAL);
  }


  public ManagedStateSpillableStateStore createStore(Configuration conf)
  {
    String basePath = getStoreBasePath(conf);
    ManagedStateSpillableStateStore store = new ManagedStateSpillableStateStore();
    ((TFileImpl.DTFileImpl)store.getFileAccess()).setBasePath(basePath);
    return store;
  }

  public String getStoreBasePath(Configuration conf)
  {
    return Preconditions.checkNotNull(conf.get(PROP_STORE_PATH),
        "base path should be specified in the properties.xml");
  }
}
