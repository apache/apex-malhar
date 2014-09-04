/*
 * Copyright (c) 2013 DataTorrent, Inc. ALL Rights Reserved.
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

import org.apache.hadoop.conf.Configuration;

import com.datatorrent.api.DAG;
import com.datatorrent.api.DAG.Locality;
import com.datatorrent.api.StreamingApplication;
import com.datatorrent.api.annotation.ApplicationAnnotation;
import com.datatorrent.contrib.hbase.HBaseCsvMappingPutOperator;
import com.datatorrent.contrib.hbase.HBaseRowStringGenerator;

/**
 * BenchMark Results
 * -----------------
 * The operator operates at 20,000 tuples/sec with the following configuration
 *
 * Container memory size=1G
 * CPU=Intel(R) Core(TM) i7-4500U CPU @ 1.80 GHz 2.40 Ghz
 *
 * @since 1.0.4
 */
@ApplicationAnnotation(name="HBaseBenchmarkApp")
public class HBaseCsvMappingApplication implements StreamingApplication
{
  private final Locality locality = null;

  @Override
  public void populateDAG(DAG dag, Configuration conf)
  {
    HBaseRowStringGenerator row = dag.addOperator("rand", new HBaseRowStringGenerator());
  

    HBaseCsvMappingPutOperator csvMappingPutOperator = dag.addOperator("HBaseoper", new HBaseCsvMappingPutOperator());
    csvMappingPutOperator.getStore().setTableName("table1");
    csvMappingPutOperator.getStore().setZookeeperQuorum("127.0.0.1");
    csvMappingPutOperator.getStore().setZookeeperClientPort(2181);
    csvMappingPutOperator.setMappingString("colfam0.street,colfam0.city,colfam0.state,row");
    dag.addStream("hbasestream",row.outputPort, csvMappingPutOperator.input).setLocality(locality);
  }

 

}
