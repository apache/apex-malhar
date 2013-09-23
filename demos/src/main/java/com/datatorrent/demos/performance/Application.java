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
package com.datatorrent.demos.performance;

import org.apache.hadoop.conf.Configuration;

import com.datatorrent.api.DAG;
import com.datatorrent.api.DAG.Locality;
import com.datatorrent.api.StreamingApplication;

/**
 * Example of application configuration in Java.<p>
 *
 * @since 0.3.2
 */
public class Application implements StreamingApplication
{
  private final Locality locality = null;
  public static final int QUEUE_CAPACITY = 32 * 1024;

  @Override
  public void populateDAG(DAG dag, Configuration conf)
  {
    RandomWordInputModule wordGenerator = dag.addOperator("wordGenerator", RandomWordInputModule.class);
   // dag.getMeta(wordGenerator).getMeta(wordGenerator.output).getAttributes().attr(PortContext.QUEUE_CAPACITY).set(QUEUE_CAPACITY);

    WordCountOperator<byte[]> counter = dag.addOperator("counter", new WordCountOperator<byte[]>());
    //dag.getMeta(counter).getMeta(counter.input).getAttributes().attr(PortContext.QUEUE_CAPACITY).set(QUEUE_CAPACITY);

    dag.addStream("Generator2Counter", wordGenerator.output, counter.input).setLocality(locality);
  }

}
