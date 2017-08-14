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

import java.io.Serializable;

import org.apache.apex.malhar.lib.algo.UniqueValueCount;
import org.apache.apex.malhar.lib.algo.UniqueValueCount.InternalCountOutput;
import org.apache.apex.malhar.lib.codec.KryoSerializableStreamCodec;
import org.apache.apex.malhar.lib.io.ConsoleOutputOperator;
import org.apache.hadoop.conf.Configuration;

import com.datatorrent.api.Context;
import com.datatorrent.api.DAG;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.api.StreamingApplication;
import com.datatorrent.api.annotation.ApplicationAnnotation;

/**
 * This application demonstrates the UniqueValueCount operator. It uses an input operator which generates random key
 * value pairs and emits them to the UniqueValueCount operator which keeps track of the unique values per window. It
 * then emits the values to the StatefulUniqueCount which uses a combination of a cache and database to keep track of
 * the overall unique values and outputs the resulting unique value count to the ConsoleOutputOperator.
 *
 * @since 1.0.4
 */
@ApplicationAnnotation(name = "StatefulDistinctCount")
public class StatefulApplication implements StreamingApplication
{
  @SuppressWarnings("unchecked")
  @Override
  public void populateDAG(DAG dag, Configuration conf)
  {
    RandomKeyValGenerator randGen = dag.addOperator("RandomGenerator", new RandomKeyValGenerator());
    UniqueValueCount<Integer> valCount = dag.addOperator("UniqueCounter", new UniqueValueCount<Integer>());
    ConsoleOutputOperator consOut = dag.addOperator("Console", new ConsoleOutputOperator());
    IntegerUniqueValueCountAppender uniqueUnifier = dag.addOperator("StatefulUniqueCounter", new IntegerUniqueValueCountAppender());
    dag.getOperatorMeta("StatefulUniqueCounter").getMeta(uniqueUnifier.input).getAttributes().put(Context.PortContext.STREAM_CODEC, new KeyBasedStreamCodec());

    @SuppressWarnings("rawtypes")
    DefaultOutputPort valOut = valCount.output;
    @SuppressWarnings("rawtypes")
    DefaultOutputPort uniqueOut = uniqueUnifier.output;

    dag.addStream("Events", randGen.outport, valCount.input);
    dag.addStream("Unified", valOut, uniqueUnifier.input);
    dag.addStream("Result", uniqueOut, consOut.input);
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
