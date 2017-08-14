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
package org.apache.apex.benchmark.accumulo;

import org.apache.accumulo.core.data.Mutation;
import org.apache.apex.malhar.contrib.accumulo.AbstractAccumuloOutputOperator;
import org.apache.apex.malhar.contrib.accumulo.AccumuloRowTupleGenerator;
import org.apache.apex.malhar.contrib.accumulo.AccumuloTestHelper;
import org.apache.apex.malhar.contrib.accumulo.AccumuloTuple;
import org.apache.hadoop.conf.Configuration;

import com.datatorrent.api.DAG;
import com.datatorrent.api.StreamingApplication;

/**
 * BenchMark Results
 * -----------------
 * The operator operates at 30,000 tuples/sec with the following configuration
 *
 * Container memory size=1G
 * Accumulo Max Memory=2G
 * Accumulo number of write threads=1
 * CPU=Intel(R) Core(TM) i7-4500U CPU @ 1.80 GHz 2.40 Ghz
 *
 * @since 1.0.4
 */
public class AccumuloApp implements StreamingApplication
{

  @Override
  public void populateDAG(DAG dag, Configuration conf)
  {
    AccumuloTestHelper.getConnector();
    AccumuloTestHelper.clearTable();
    dag.setAttribute(DAG.APPLICATION_NAME, "AccumuloOutputTest");
    AccumuloRowTupleGenerator rtg = dag.addOperator("tuplegenerator", AccumuloRowTupleGenerator.class);
    TestAccumuloOutputOperator taop = dag.addOperator("testaccumulooperator", TestAccumuloOutputOperator.class);
    dag.addStream("ss", rtg.outputPort, taop.input);
    com.datatorrent.api.Attribute.AttributeMap attributes = dag.getAttributes();
    taop.getStore().setTableName("tab1");
    taop.getStore().setZookeeperHost("127.0.0.1");
    taop.getStore().setInstanceName("instance");
    taop.getStore().setUserName("root");
    taop.getStore().setPassword("pass");

  }

  public static class TestAccumuloOutputOperator extends AbstractAccumuloOutputOperator<AccumuloTuple>
  {

    @Override
    public Mutation operationMutation(AccumuloTuple t)
    {
      Mutation mutation = new Mutation(t.getRow().getBytes());
      mutation.put(t.getColFamily().getBytes(), t.getColName().getBytes(), t.getColValue().getBytes());
      return mutation;
    }

  }
}
