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
package com.datatorrent.benchmark.algo;

import com.datatorrent.api.Context;
import com.datatorrent.api.DAG;
import com.datatorrent.api.StreamingApplication;
import com.datatorrent.api.annotation.ApplicationAnnotation;
import com.datatorrent.lib.algo.PartitionableUniqueCount;
import com.datatorrent.lib.algo.UniqueCounterValue;
import com.datatorrent.lib.io.ConsoleOutputOperator;
import com.datatorrent.lib.testbench.RandomEventGenerator;
import org.apache.hadoop.conf.Configuration;

/**
 * Application to demonstrate PartitionableUniqueCount operator. <br>
 * The input operator generate random keys, which is sent to
 * PartitionableUniqueCount operator initially partitioned into three partitions to
 * test unifier functionality, and output of the operator is sent to verifier to verify
 * that it generates correct result.
 *
 * @since 1.0.2
 */
@ApplicationAnnotation(name = "UniqueCountBenchmark")
public class UniqueValueCountBenchmarkApplication implements StreamingApplication
{

  @Override
  public void populateDAG(DAG dag, Configuration entries)
  {

    dag.setAttribute(dag.APPLICATION_NAME, "UniqueValueCountDemo");
    dag.setAttribute(dag.DEBUG, true);


    /* Generate random key-value pairs */
    RandomEventGenerator randGen = dag.addOperator("randomgen", new RandomEventGenerator());
    randGen.setMaxvalue(999999);
    randGen.setTuplesBlastIntervalMillis(50);
    dag.setAttribute(randGen, Context.OperatorContext.INITIAL_PARTITION_COUNT, 3);

    /* Initialize with three partition to start with */
    // UniqueCount1 uniqCount = dag.addOperator("uniqevalue", new UniqueCount1());
    PartitionableUniqueCount<Integer> uniqCount = dag.addOperator("uniqevalue", new PartitionableUniqueCount<Integer>());
    dag.setAttribute(uniqCount, Context.OperatorContext.INITIAL_PARTITION_COUNT, 3);
    dag.setInputPortAttribute(uniqCount.data, Context.PortContext.PARTITION_PARALLEL, true);
    uniqCount.setCumulative(false);

    UniqueCounterValue counter = dag.addOperator("count", new UniqueCounterValue());
    ConsoleOutputOperator output = dag.addOperator("output", new ConsoleOutputOperator());

    dag.addStream("datain", randGen.integer_data, uniqCount.data);
    dag.addStream("consoutput", uniqCount.count, counter.data);
    dag.addStream("final", counter.count, output.input);
  }

}
