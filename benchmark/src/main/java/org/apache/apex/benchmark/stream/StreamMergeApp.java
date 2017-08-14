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
package org.apache.apex.benchmark.stream;

import org.apache.apex.benchmark.WordCountOperator;
import org.apache.apex.malhar.lib.stream.StreamMerger;
import org.apache.hadoop.conf.Configuration;

import com.datatorrent.api.Context.PortContext;
import com.datatorrent.api.DAG;
import com.datatorrent.api.DAG.Locality;
import com.datatorrent.api.StreamingApplication;
import com.datatorrent.api.annotation.ApplicationAnnotation;

/**
 * Benchmark App for StreamMerge Operator.
 * This operator is benchmarked to emit 1,300,000 tuples/sec on cluster node.
 *
 * @since 2.0.0
 */
@ApplicationAnnotation(name = "StreamMergeApp")
public class StreamMergeApp implements StreamingApplication
{
  private final Locality locality = null;
  public static final int QUEUE_CAPACITY = 16 * 1024;

  @Override
  public void populateDAG(DAG dag, Configuration conf)
  {
    IntegerOperator intInput = dag.addOperator("intInput", new IntegerOperator());
    StreamMerger stream = dag.addOperator("stream", new StreamMerger());
    dag.getMeta(stream).getMeta(stream.data1).getAttributes().put(PortContext.QUEUE_CAPACITY, QUEUE_CAPACITY);
    dag.getMeta(stream).getMeta(stream.data2).getAttributes().put(PortContext.QUEUE_CAPACITY, QUEUE_CAPACITY);
    dag.addStream("streammerge1", intInput.integer_data, stream.data1, stream.data2).setLocality(locality);

    WordCountOperator<Integer> counter = dag.addOperator("counter", new WordCountOperator<Integer>());
    dag.getMeta(counter).getMeta(counter.input).getAttributes().put(PortContext.QUEUE_CAPACITY, QUEUE_CAPACITY);
    dag.getMeta(stream).getMeta(stream.out).getAttributes().put(PortContext.QUEUE_CAPACITY, QUEUE_CAPACITY);
    dag.addStream("streammerge2", stream.out, counter.input).setLocality(locality);

  }

}
