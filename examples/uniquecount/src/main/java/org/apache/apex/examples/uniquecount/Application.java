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
package org.apache.apex.examples.uniquecount;

import org.apache.apex.malhar.lib.algo.UniqueCounter;
import org.apache.apex.malhar.lib.converter.MapToKeyHashValuePairConverter;
import org.apache.apex.malhar.lib.io.ConsoleOutputOperator;
import org.apache.apex.malhar.lib.stream.Counter;
import org.apache.apex.malhar.lib.stream.StreamDuplicater;
import org.apache.apex.malhar.lib.util.KeyHashValPair;
import org.apache.hadoop.conf.Configuration;

import com.datatorrent.api.Context;
import com.datatorrent.api.DAG;
import com.datatorrent.api.DAG.Locality;
import com.datatorrent.api.StreamingApplication;
import com.datatorrent.api.annotation.ApplicationAnnotation;

import com.datatorrent.common.partitioner.StatelessPartitioner;

/**
 * Application to demonstrate PartitionableUniqueCount operator. <br>
 * The input operator generate random keys, which is sent to
 * PartitionableUniqueCount operator initially partitioned into three partitions to
 * test unifier functionality, and output of the operator is sent to verifier to verify
 * that it generates correct result.
 *
 * @since 1.0.2
 */
@ApplicationAnnotation(name = "UniqueValueCountExample")
public class Application implements StreamingApplication
{

  @Override
  public void populateDAG(DAG dag, Configuration entries)
  {
        /* Generate random key-value pairs */
    RandomKeysGenerator randGen = dag.addOperator("randomgen", new RandomKeysGenerator());


        /* Initialize with three partition to start with */
    // UniqueCount1 uniqCount = dag.addOperator("uniqevalue", new UniqueCount1());
    UniqueCounter<Integer> uniqCount = dag.addOperator("uniqevalue", new UniqueCounter<Integer>());

    MapToKeyHashValuePairConverter<Integer, Integer> converter = dag.addOperator("converter", new MapToKeyHashValuePairConverter());

    uniqCount.setCumulative(false);
    dag.setAttribute(uniqCount, Context.OperatorContext.PARTITIONER, new StatelessPartitioner<UniqueCounter<Integer>>(3));

    CountVerifier<Integer> verifier = dag.addOperator("verifier", new CountVerifier<Integer>());
    StreamDuplicater<KeyHashValPair<Integer, Integer>> dup = dag.addOperator("dup", new StreamDuplicater<KeyHashValPair<Integer, Integer>>());
    ConsoleOutputOperator output = dag.addOperator("output", new ConsoleOutputOperator());

    ConsoleOutputOperator successOutput = dag.addOperator("successoutput", new ConsoleOutputOperator());
    successOutput.setStringFormat("Success %d");
    ConsoleOutputOperator failureOutput = dag.addOperator("failureoutput", new ConsoleOutputOperator());
    failureOutput.setStringFormat("Failure %d");

    // success and failure counters.
    Counter successcounter = dag.addOperator("successcounter", new Counter());
    Counter failurecounter = dag.addOperator("failurecounter", new Counter());

    dag.addStream("datain", randGen.outPort, uniqCount.data);
    dag.addStream("dataverification0", randGen.verificationPort, verifier.in1);
    dag.addStream("convert", uniqCount.count, converter.input).setLocality(Locality.THREAD_LOCAL);
    dag.addStream("split", converter.output, dup.data);
    dag.addStream("consoutput", dup.out1, output.input);
    dag.addStream("dataverification1", dup.out2, verifier.in2);
    dag.addStream("successc", verifier.successPort, successcounter.input);
    dag.addStream("failurec", verifier.failurePort, failurecounter.input);
    dag.addStream("succconsoutput", successcounter.output, successOutput.input);
    dag.addStream("failconsoutput", failurecounter.output, failureOutput.input);
  }
}
