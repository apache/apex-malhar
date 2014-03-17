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
package com.datatorrent.benchmark;

import org.apache.hadoop.conf.Configuration;

import com.datatorrent.api.Context.PortContext;
import com.datatorrent.api.DAG;
import com.datatorrent.api.DAG.Locality;
import com.datatorrent.api.StreamingApplication;
import com.datatorrent.api.annotation.ApplicationAnnotation;

/**
 * Performance Demo Application:
 * <p>
 * This demo demonstrates the performance of Datatorrent platform.
 * Performance is measured by the number of events processed per second and latency.
 * Performance varies depending on container memory, CPU and network I/O.
 * The demo can be used to check how the performance varies with stream locality.
 *
 * Stream locality decides how the operators are deployed:
 * ThreadLocal - the operators are deployed within the same thread.
 * ContainerLocal -the operators are deployed as separate threads within the process.
 * NodeLocal- the operators are deployed as separate processes on a machine.
 * RackLocal - the operators are deployed on different nodes of the same rack.
 * NoLocality - lets the engine decide how to best deploy the operator.
 *
 * Note:  NodeLocal and RackLocal are preferences that can be specified to Hadoop ResourceManager.
 * It is not guaranteed that the operators will be deployed as requested.
 * ResourceManager makes the call depending on resource availability.
 *
 * Refer to demos/docs/PerformanceDemo.md for more details.
 *
 * </p>
 *
 * @since 0.9.0
 */

@ApplicationAnnotation(name="PerformanceBenchmarkingApp")
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
  @ApplicationAnnotation(name="PerformanceBenchmarkNoLocality")
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
   * The operators are requested to be deployed on different machines.
   */
  @ApplicationAnnotation(name="PerformanceBenchmarkRackLocal")
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
   * The operators are requested to be deployed as different processes within the same machine.
   */  
  @ApplicationAnnotation(name="PerformanceBenchmarkNodeLocal")
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
   * The operators are deployed as different threads in the same process.
   */
  @ApplicationAnnotation(name="PerformanceBenchmarkContainerLocal")
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
  @ApplicationAnnotation(name="PerformanceBenchmarkThreadLocal")
  public static class ThreadLocal extends AbstractApplication
  {
    @Override
    public Locality getLocality()
    {
      return Locality.THREAD_LOCAL;
    }

  }

}
