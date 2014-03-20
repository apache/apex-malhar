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
package com.datatorrent.demos.scalability;

import com.datatorrent.api.Context.OperatorContext;
import com.datatorrent.api.Context.PortContext;
import com.datatorrent.api.DAG;
import com.datatorrent.api.DAG.Locality;
import com.datatorrent.api.annotation.ApplicationAnnotation;
import com.datatorrent.api.StreamingApplication;
import com.datatorrent.lib.io.ConsoleOutputOperator;
import com.datatorrent.lib.io.MapMultiConsoleOutputOperator;

import java.util.Map;

import org.apache.commons.lang.mutable.MutableDouble;
import org.apache.hadoop.conf.Configuration;

/**
 * <p>Application class.</p>
 *
 * @since 0.3.2
 */
@ApplicationAnnotation(name="ScalableAdsApplication")
public class ScalableAdsApp implements StreamingApplication
{
  
  private int QUEUE_CAPACITY= 16*1024;
  public static final int WINDOW_SIZE_MILLIS = 1000;

  @Override
  public void populateDAG(DAG dag, Configuration conf)
  {
    dag.setAttribute(DAG.APPLICATION_NAME, "ScalableAdsApplication");
    dag.setAttribute(DAG.STREAMING_WINDOW_SIZE_MILLIS, WINDOW_SIZE_MILLIS);
    
    int heartbeat_interval = conf.getInt(ScalableAdsApp.class.getName() + ".heartbeat_interval", 1000);
    int heartbeat_timeout = conf.getInt(ScalableAdsApp.class.getName() + ".heartbeat_timeout", 30000);
    int unifier_count = conf.getInt(ScalableAdsApp.class.getName() + ".unifier_count", 2);
    
    dag.setAttribute(DAG.HEARTBEAT_INTERVAL_MILLIS, heartbeat_interval);
    dag.setAttribute(DAG.HEARTBEAT_TIMEOUT_MILLIS, heartbeat_timeout);

    int partitions = conf.getInt(ScalableAdsApp.class.getName() + ".partitions", 1);
    int partitions_agg = conf.getInt(ScalableAdsApp.class.getName() + ".partitions_agg", 1);
    
    InputItemGenerator input = dag.addOperator("input", InputItemGenerator.class);
    dag.setOutputPortAttribute(input.outputPort, PortContext.QUEUE_CAPACITY, QUEUE_CAPACITY);
    dag.setAttribute(input, OperatorContext.INITIAL_PARTITION_COUNT, partitions);

    InputDimensionGenerator inputDimension = dag.addOperator("inputDimension", InputDimensionGenerator.class);
    dag.setInputPortAttribute(inputDimension.inputPort, PortContext.PARTITION_PARALLEL, true);
    dag.setInputPortAttribute(inputDimension.inputPort, PortContext.QUEUE_CAPACITY, QUEUE_CAPACITY);
    dag.setOutputPortAttribute(inputDimension.outputPort, PortContext.QUEUE_CAPACITY, QUEUE_CAPACITY);

    BucketOperator bucket = dag.addOperator("bucket", BucketOperator.class);
    bucket.setPartitions(partitions_agg);
    dag.setInputPortAttribute(bucket.inputPort, PortContext.PARTITION_PARALLEL, true);
    dag.setInputPortAttribute(bucket.inputPort, PortContext.QUEUE_CAPACITY, QUEUE_CAPACITY);
    dag.setOutputPortAttribute(bucket.outputPort, PortContext.QUEUE_CAPACITY, QUEUE_CAPACITY);
    //TODO: uncomment after latest version
    dag.setOutputPortAttribute(bucket.outputPort, PortContext.UNIFIER_LIMIT,unifier_count);
    dag.setAttribute(bucket, OperatorContext.APPLICATION_WINDOW_COUNT, 5);

    //MapMultiConsoleOutputOperator<AggrKey, Object> console = dag.addOperator("console", MapMultiConsoleOutputOperator.class);
    ConsoleOutputOperator console = dag.addOperator("console", ConsoleOutputOperator.class);
    dag.setAttribute(console, OperatorContext.INITIAL_PARTITION_COUNT, partitions_agg);
    console.silent= true;
    console.setDebug(false);
    dag.setInputPortAttribute(console.input, PortContext.QUEUE_CAPACITY, QUEUE_CAPACITY);

    dag.addStream("ingen", input.outputPort, inputDimension.inputPort).setLocality(Locality.CONTAINER_LOCAL);
    dag.addStream("indimgen", inputDimension.outputPort, bucket.inputPort).setLocality(Locality.THREAD_LOCAL);
    dag.addStream("console", bucket.outputPort, console.input);
    //dag.addStream("console", bucket.outputPort, console.input).setLocality(Locality.CONTAINER_LOCAL);
  }
}
