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
package com.datatorrent.contrib.adsdimension;

import com.datatorrent.api.Context.OperatorContext;
import com.datatorrent.api.Context.PortContext;
import com.datatorrent.api.DAG;
import com.datatorrent.api.DAG.Locality;
import com.datatorrent.api.StreamingApplication;
import com.datatorrent.contrib.redis.RedisNumberSummationKeyValPairOutputOperator;
import java.util.Map;
import org.apache.commons.lang.mutable.MutableDouble;
import org.apache.hadoop.conf.Configuration;

/**
 * <p>Application class.</p>
 *
 * @since 0.3.2
 */
public class Application implements StreamingApplication
{

  @Override
  public void populateDAG(DAG dag, Configuration conf)
  {
    dag.setAttribute(DAG.APPLICATION_NAME, "AdsDimensionApplication");

    InputItemGenerator input = dag.addOperator("InputGenerator", InputItemGenerator.class);
    dag.setOutputPortAttribute(input.outputPort, PortContext.QUEUE_CAPACITY, 32 * 1024);
    dag.setAttribute(input, OperatorContext.INITIAL_PARTITION_COUNT, 8);

    InputDimensionGenerator inputDimension = dag.addOperator("DimensionalDataGenerator", InputDimensionGenerator.class);
    dag.setInputPortAttribute(inputDimension.inputPort, PortContext.PARTITION_PARALLEL, true);
    dag.setInputPortAttribute(inputDimension.inputPort, PortContext.QUEUE_CAPACITY, 32 * 1024);
    dag.setOutputPortAttribute(inputDimension.outputPort, PortContext.QUEUE_CAPACITY, 32 * 1024);

    BucketOperator bucket = dag.addOperator("MinuteBucketAggregator", BucketOperator.class);
    dag.setInputPortAttribute(bucket.inputPort, PortContext.PARTITION_PARALLEL, true);
    dag.setInputPortAttribute(bucket.inputPort, PortContext.QUEUE_CAPACITY, 32 * 1024);
    dag.setOutputPortAttribute(bucket.outputPort, PortContext.QUEUE_CAPACITY, 32 * 1024);
    dag.setAttribute(bucket, OperatorContext.APPLICATION_WINDOW_COUNT, 10);

    RedisNumberSummationKeyValPairOutputOperator<AggrKey, Map<String, MutableDouble>> redis = dag.addOperator("RedisAdapter", new RedisNumberSummationKeyValPairOutputOperator<AggrKey, Map<String, MutableDouble>>());
    dag.setInputPortAttribute(redis.input, PortContext.QUEUE_CAPACITY, 32 * 1024);
    dag.setAttribute(redis, OperatorContext.INITIAL_PARTITION_COUNT, 2);

    dag.addStream("InputStream", input.outputPort, inputDimension.inputPort).setLocality(Locality.CONTAINER_LOCAL);
    dag.addStream("DimensionalData", inputDimension.outputPort, bucket.inputPort).setLocality(Locality.CONTAINER_LOCAL);
    dag.addStream("AggregateData", bucket.outputPort, redis.input);
  }
}
