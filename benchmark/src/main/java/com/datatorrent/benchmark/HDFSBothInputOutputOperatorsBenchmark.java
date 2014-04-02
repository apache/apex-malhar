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
import com.datatorrent.api.Context.PortContext;
import com.datatorrent.api.DAG.Locality;
import com.datatorrent.api.annotation.ApplicationAnnotation;
import com.datatorrent.lib.io.fs.HdfsWordInputOperator;

import org.apache.hadoop.conf.Configuration;

/**
 * Application used to benchmark both HDFS input and HDFS output operators 
 * connected in a DAG<p>
 *
 * @since 0.3.2
 */

@ApplicationAnnotation(name="HDFSBothInOutOperatorsBenchmarkingApp")
public abstract class HDFSBothInputOutputOperatorsBenchmark
{
  static abstract class AbstractApplication implements StreamingApplication
  {
    static final int QUEUE_CAPACITY = 32 * 1024;

    @Override
    public void populateDAG(DAG dag, Configuration conf)
    {
      HdfsWordInputOperator wordGenerator =  dag.addOperator("wordGenerator", HdfsWordInputOperator.class);
      wordGenerator.setFilePath("hdfs:///user/hadoop/hdfsOperatorBenchmarking/2/transactions.out.part0");
      dag.getMeta(wordGenerator).getMeta(wordGenerator.output).getAttributes().put(PortContext.QUEUE_CAPACITY, QUEUE_CAPACITY);
   
      HdfsByteOutputOperator hdfsOutputOperator = dag.addOperator("hdfsOutputOperator", new HdfsByteOutputOperator());
      hdfsOutputOperator.setFilePathPattern("hdfsBothInputOutputOperatorBenchmarking" + "/%(contextId)/transactions.out.part%(partIndex)");
      hdfsOutputOperator.setAppend(false);
      dag.getMeta(wordGenerator).getMeta(wordGenerator.output).getAttributes().put(PortContext.QUEUE_CAPACITY, QUEUE_CAPACITY);
 
      dag.addStream("HDFSInput2HDFSOutput", wordGenerator.output, hdfsOutputOperator.input).setLocality(getLocality());      
    }
    
    public abstract Locality getLocality();
    
  }
  
  /**
   * Let the engine decide how to best place the 2 operators.
   */
  @ApplicationAnnotation(name="HDFSBothInOutOperatorsBenchmarkNoLocality")
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
  @ApplicationAnnotation(name="HDFSBothInOutOperatorsBenchmarkRackLocality")
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
  @ApplicationAnnotation(name="HDFSBothInOutOperatorsBenchmarkNodeLocality")
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
  @ApplicationAnnotation(name="HDFSBothInOutOperatorsBenchmarkContainerLocality")
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
  @ApplicationAnnotation(name="HDFSBothInOutOperatorsBenchmarkThreadLocality")
  public static class ThreadLocal extends AbstractApplication
  {
    @Override
    public Locality getLocality()
    {
      return Locality.THREAD_LOCAL;
    }
  }

}


