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

import com.datatorrent.api.StreamingApplication;
import com.datatorrent.api.DAG;
import com.datatorrent.api.DAG.Locality;
import com.datatorrent.lib.io.fs.HdfsOutputOperator;

import org.apache.hadoop.conf.Configuration;

/**
 * Application used to benchmark HDFS output operator
 * The DAG consists of random word generator operator that is 
 * connected to HDFS output operator that writes to a file on HDFS.<p>
 *
 * @since 0.9.4
 */

public abstract class HDFSOutputOperatorBenchmark
{
  static abstract class AbstractApplication implements StreamingApplication
  {
    public static final int QUEUE_CAPACITY = 32 * 1024;

    @Override
    public void populateDAG(DAG dag, Configuration conf)
    {
      RandomWordInputModule wordGenerator = dag.addOperator("wordGenerator", RandomWordInputModule.class);
   
      HdfsOutputOperator hdfsOutputOperator = dag.addOperator("hdfsOutputOperator", new HdfsOutputOperator());
      hdfsOutputOperator.setFilePath("hdfsOperatorBenchmarking" + "/%(contextId)/transactions.out.part%(partIndex)");
      hdfsOutputOperator.setAppend(false);
   
      dag.addStream("Generator2HDFSOutput", wordGenerator.output, hdfsOutputOperator.input).setLocality(getLocality());      
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


