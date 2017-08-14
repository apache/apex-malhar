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
package org.apache.apex.benchmark.memsql;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.apex.malhar.contrib.memsql.MemsqlInputOperator;
import org.apache.apex.malhar.lib.stream.DevNull;
import org.apache.hadoop.conf.Configuration;

import com.datatorrent.api.DAG;
import com.datatorrent.api.StreamingApplication;
import com.datatorrent.api.annotation.ApplicationAnnotation;

/**
 * BenchMark Results
 * -----------------
 * The operator operates at 450,000 tuples/sec with the following configuration
 *
 * Container memory size=1G
 * Default memsql configuration
 * memsql number of write threads=1
 * batch size 1000
 * 1 master aggregator, 1 leaf
 *
 * @since 1.0.5
 */
@ApplicationAnnotation(name = "MemsqlInputBenchmark")
public class MemsqlInputBenchmark implements StreamingApplication
{
  private static final Logger LOG = LoggerFactory.getLogger(MemsqlInputBenchmark.class);

  public String host = null;

  @Override
  public void populateDAG(DAG dag, Configuration conf)
  {
    MemsqlInputOperator memsqlInputOperator = dag.addOperator("memsqlInputOperator",
        new MemsqlInputOperator());

    DevNull<Object> devNull = dag.addOperator("devnull",
        new DevNull<Object>());

    dag.addStream("memsqlconnector",
        memsqlInputOperator.outputPort,
        devNull.data);
  }
}
