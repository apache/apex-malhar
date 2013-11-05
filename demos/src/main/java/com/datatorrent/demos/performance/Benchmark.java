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

import com.datatorrent.api.Context.PortContext;
import com.datatorrent.api.DAG;
import com.datatorrent.api.DAG.Locality;
import com.datatorrent.api.StreamingApplication;

/**
 * Benchmark for operator locality.<p>
 *
 * @since 0.9.0
 */
public abstract class Benchmark
{
  static abstract class AbstractApplication implements StreamingApplication
  {
    public static final int QUEUE_CAPACITY = 32 * 1024;

    @Override
    public void populateDAG(DAG dag, Configuration conf)
    {
      RandomWordInputModule wordGenerator = dag.addOperator("wordGenerator", RandomWordInputModule.class);
      dag.getMeta(wordGenerator).getMeta(wordGenerator.output).getAttributes().put(PortContext.QUEUE_CAPACITY, QUEUE_CAPACITY);

      WordCountOperator<byte[]> counter = dag.addOperator("counter", new WordCountOperator<byte[]>());
      dag.getMeta(counter).getMeta(counter.input).getAttributes().put(PortContext.QUEUE_CAPACITY, QUEUE_CAPACITY);

      dag.addStream("Generator2Counter", wordGenerator.output, counter.input).setLocality(getLocality());
    }

    public abstract Locality getLocality();
  }
  
  /**
   * Let the engine decide how to best place the 2 operators.
   */
  public static class NoLocality extends AbstractApplication
  {
    @Override
    public Locality getLocality()
    {
      return null;
    }

  }

  /**
   * Place the 2 operators so that they are in the same Rack.
   */
  public static class RackLocal extends AbstractApplication
  {
    @Override
    public Locality getLocality()
    {
      return Locality.RACK_LOCAL;
    }

  }

  /**
   * Place the 2 operators so that they are in the same node.
   */
  public static class NodeLocal extends AbstractApplication
  {
    @Override
    public Locality getLocality()
    {
      return Locality.NODE_LOCAL;
    }

  }

  /**
   * Place the 2 operators so that they are in the same container.
   */
  public static class ContainerLocal extends AbstractApplication
  {
    @Override
    public Locality getLocality()
    {
      return Locality.CONTAINER_LOCAL;
    }

  }

  /**
   * Place the 2 operators so that they are in the same thread.
   */
  public static class ThreadLocal extends AbstractApplication
  {
    @Override
    public Locality getLocality()
    {
      return Locality.THREAD_LOCAL;
    }

  }

}
