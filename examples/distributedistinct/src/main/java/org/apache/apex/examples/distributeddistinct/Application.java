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
package org.apache.apex.examples.distributeddistinct;

import org.apache.apex.malhar.lib.algo.UniqueValueCount;
import org.apache.apex.malhar.lib.io.ConsoleOutputOperator;
import org.apache.apex.malhar.lib.stream.Counter;
import org.apache.apex.malhar.lib.stream.StreamDuplicater;
import org.apache.apex.malhar.lib.util.KeyValPair;
import org.apache.hadoop.conf.Configuration;

import com.datatorrent.api.DAG;
import com.datatorrent.api.StreamingApplication;
import com.datatorrent.api.annotation.ApplicationAnnotation;

/**
 * This application demonstrates the UniqueValueCount operator. It uses an input operator which generates random key
 * value pairs and simultaneously emits them to the UniqueValueCount operator and keeps track of the number of unique
 * values per key to emit to the verifier.
 *
 * @since 1.0.4
 */
@ApplicationAnnotation(name = "ValueCount")
public class Application implements StreamingApplication
{

  @Override
  public void populateDAG(DAG dag, Configuration conf)
  {
    RandomKeyValGenerator randGen = dag.addOperator("RandomGenerator", new RandomKeyValGenerator());
    UniqueValueCount<Integer> valCount = dag.addOperator("UniqueCounter", new UniqueValueCount<Integer>());
    ConsoleOutputOperator consOut = dag.addOperator("Console", new ConsoleOutputOperator());
    StreamDuplicater<KeyValPair<Integer, Integer>> dup = dag.addOperator("Duplicator", new StreamDuplicater<KeyValPair<Integer, Integer>>());
    CountVerifier verifier = dag.addOperator("Verifier", new CountVerifier());
    ConsoleOutputOperator successOutput = dag.addOperator("Success", new ConsoleOutputOperator());
    successOutput.setStringFormat("Success %d");
    ConsoleOutputOperator failureOutput = dag.addOperator("Failure", new ConsoleOutputOperator());
    failureOutput.setStringFormat("Failure %d");

    Counter successcounter = dag.addOperator("SuccessCounter", new Counter());
    Counter failurecounter = dag.addOperator("FailureCounter", new Counter());

    dag.addStream("Events", randGen.outport, valCount.input);
    dag.addStream("Duplicates", valCount.output, dup.data);
    dag.addStream("Unverified", dup.out1, verifier.recIn);
    dag.addStream("EventCount", randGen.verport, verifier.trueIn);
    dag.addStream("Verified", verifier.successPort, successcounter.input);
    dag.addStream("Failed", verifier.failurePort, failurecounter.input);
    dag.addStream("SuccessCount", successcounter.output, successOutput.input);
    dag.addStream("FailedCount", failurecounter.output, failureOutput.input);
    dag.addStream("Output", dup.out2, consOut.input);
  }
}
