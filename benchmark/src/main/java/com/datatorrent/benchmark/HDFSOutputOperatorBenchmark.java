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

import com.datatorrent.api.StreamingApplication;
import com.datatorrent.api.DAG;
import com.datatorrent.api.DAG.Locality;
import com.datatorrent.api.annotation.ApplicationAnnotation;


import org.apache.hadoop.conf.Configuration;

/**
 * Application used to benchmark HDFS output operator
 * The DAG consists of random word generator operator that is 
 * connected to HDFS output operator that writes to a file on HDFS.<p>
 *
 * @since 0.9.4
 */

@ApplicationAnnotation(name="HDFSOutputOperatorBenchmarkingApp")
public abstract class HDFSOutputOperatorBenchmark
{
  static abstract class AbstractApplication implements StreamingApplication
  {
    public static final int QUEUE_CAPACITY = 32 * 1024;

    @Override
    public void populateDAG(DAG dag, Configuration conf)
    {
      RandomWordInputModule wordGenerator = dag.addOperator("wordGenerator", RandomWordInputModule.class);
   
      HdfsByteOutputOperator hdfsOutputOperator = dag.addOperator("hdfsOutputOperator", new HdfsByteOutputOperator());
      hdfsOutputOperator.setFilePathPattern("hdfsOperatorBenchmarking" + "/%(contextId)/transactions.out.part%(partIndex)");
      hdfsOutputOperator.setAppend(false);
   
      dag.addStream("Generator2HDFSOutput", wordGenerator.output, hdfsOutputOperator.input).setLocality(getLocality());      
    }
    
    public abstract Locality getLocality();
    
  }
  
  /**
   * Let the engine decide how to best place the 2 operators.
   */
  @ApplicationAnnotation(name="HDFSOutputOperatorBenchmarkNoLocality")
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
  @ApplicationAnnotation(name="HDFSOutputOperatorBenchmarkRackLocality")
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
  @ApplicationAnnotation(name="HDFSOutputOperatorBenchmarkNodeLocality")
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
  @ApplicationAnnotation(name="HDFSOutputOperatorBenchmarkContainerLocality")
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
  @ApplicationAnnotation(name="HDFSOutputOperatorBenchmarkThreadLocality")
  public static class ThreadLocal extends AbstractApplication
  {
    @Override
    public Locality getLocality()
    {
      return Locality.THREAD_LOCAL;
    }
  }

}


