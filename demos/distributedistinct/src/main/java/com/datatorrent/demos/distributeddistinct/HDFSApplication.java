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
package com.datatorrent.demos.distributeddistinct;

import java.io.Serializable;

import org.apache.hadoop.conf.Configuration;

import com.datatorrent.api.Context;
import com.datatorrent.api.DAG;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.api.StreamingApplication;
import com.datatorrent.api.annotation.ApplicationAnnotation;
import com.datatorrent.lib.algo.UniqueCounterValue;
import com.datatorrent.lib.algo.UniqueValueCount;
import com.datatorrent.lib.algo.UniqueValueCount.InternalCountOutput;
import com.datatorrent.lib.bucket.BucketManagerImpl;
import com.datatorrent.lib.bucket.HdfsBucketStore;
import com.datatorrent.lib.codec.KryoSerializableStreamCodec;
import com.datatorrent.lib.io.ConsoleOutputOperator;


@ApplicationAnnotation(name = "StatefulHdfsDistinctCount")
public class HDFSApplication implements StreamingApplication
{
  @SuppressWarnings("unchecked")
  @Override
  public void populateDAG(DAG dag, Configuration conf)
  {
    RandomKeyValGenerator randGen = dag.addOperator("RandomGenerator", new RandomKeyValGenerator());
    UniqueValueCount<Integer> valCount = dag.addOperator("UniqueCounter", new UniqueValueCount<Integer>());
    //ConsoleOutputOperator consOut = dag.addOperator("Console", new ConsoleOutputOperator());
    HDFSUniqueValueCountAppender<Integer> uniqueUnifier = dag.addOperator("StatefulUniqueCounter", new HDFSUniqueValueCountAppender<Integer>());
    CountVerifier countVerifier = dag.addOperator("CountVerifier", new CountVerifier());
    dag.getOperatorMeta("StatefulUniqueCounter").getMeta(uniqueUnifier.input).getAttributes().put(Context.PortContext.STREAM_CODEC, new KeyBasedStreamCodec());
    dag.setAttribute(uniqueUnifier, Context.OperatorContext.INITIAL_PARTITION_COUNT, 3);
    BucketManagerImpl<BucketableInternalCountOutput<Integer>> bucketManager = new BucketManagerImpl<BucketableInternalCountOutput<Integer>>();
    HdfsBucketStore<BucketableInternalCountOutput<Integer>> bucketStore = new HdfsBucketStore<BucketableInternalCountOutput<Integer>>();
    bucketManager.setWriteEventKeysOnly(false);
    bucketManager.setNoOfBuckets(100);
    bucketManager.setNoOfBucketsInMemory(50);
    bucketManager.setMaxNoOfBucketsInMemory(50);
    bucketManager.setBucketStore(bucketStore);
    uniqueUnifier.setBucketManager(bucketManager);

    @SuppressWarnings("rawtypes")
    DefaultOutputPort valOut = valCount.output;
    @SuppressWarnings("rawtypes")
    DefaultOutputPort uniqueOut = uniqueUnifier.output;
    UniqueCounterValue<Integer> successcounter = dag.addOperator("successcounter", new UniqueCounterValue<Integer>());
    UniqueCounterValue<Integer> failurecounter = dag.addOperator("failurecounter", new UniqueCounterValue<Integer>());
    
    ConsoleOutputOperator successOutput = dag.addOperator("successoutput", new ConsoleOutputOperator());
    successOutput.setStringFormat("Success %d");
    ConsoleOutputOperator failureOutput = dag.addOperator("failureoutput", new ConsoleOutputOperator());
    failureOutput.setStringFormat("Failure %d");

    dag.addStream("Events", randGen.outport, valCount.input);
    dag.addStream("Unified", valOut, uniqueUnifier.input);
    dag.addStream("Result", uniqueOut, countVerifier.recIn);
    dag.addStream("TrueCount", randGen.verport, countVerifier.trueIn);
    dag.addStream("successc", countVerifier.successPort, successcounter.data);
    dag.addStream("failurec", countVerifier.failurePort, failurecounter.data);
    dag.addStream("succconsoutput", successcounter.count, successOutput.input);
    dag.addStream("failconsoutput", failurecounter.count, failureOutput.input);
  }

  public static class KeyBasedStreamCodec extends KryoSerializableStreamCodec<InternalCountOutput<Integer>> implements Serializable
  {
    @Override
    public int getPartition(InternalCountOutput<Integer> t)
    {
      return t.getKey().hashCode();
    }

    private static final long serialVersionUID = 201407231527L;
  }
}
