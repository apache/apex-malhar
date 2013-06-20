/*
 *  Copyright (c) 2012-2013 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.datatorrent.contrib.summit.ads;

import com.datatorrent.api.Context.OperatorContext;
import com.datatorrent.api.Context.PortContext;
import com.datatorrent.api.DAG;
import com.datatorrent.api.StreamingApplication;
import com.datatorrent.contrib.redis.RedisNumberAggregateOutputOperator;
import java.util.Map;
import org.apache.commons.lang.mutable.MutableDouble;
import org.apache.hadoop.conf.Configuration;

/**
 *
 * @author Pramod Immaneni <pramod@malhar-inc.com>
 */
public class Application implements StreamingApplication
{

  @Override
  public void populateDAG(DAG dag, Configuration conf)
  {
    dag.setAttribute(DAG.APPLICATION_NAME, "AdsDimension");

    InputGenerator input = dag.addOperator("input", InputGenerator.class);
    dag.setOutputPortAttribute(input.outputPort, PortContext.QUEUE_CAPACITY, 32 * 1024);
    dag.setAttribute(input, OperatorContext.INITIAL_PARTITION_COUNT, 9);
    BucketOperator bucket = dag.addOperator("bucket", BucketOperator.class);
    dag.setInputPortAttribute(bucket.inputPort, PortContext.PARTITION_PARALLEL, true);

    dag.setInputPortAttribute(bucket.inputPort, PortContext.QUEUE_CAPACITY, 32 * 1024);
    dag.setOutputPortAttribute(bucket.outputPort, PortContext.QUEUE_CAPACITY, 32 * 1024);
    dag.setAttribute(bucket, OperatorContext.APPLICATION_WINDOW_COUNT, 10);

    RedisNumberAggregateOutputOperator<AggrKey, Map<String, MutableDouble>> redis = dag.addOperator("redis", RedisNumberAggregateOutputOperator.class);
    dag.setInputPortAttribute(redis.input, PortContext.QUEUE_CAPACITY, 32 * 1024);
    dag.setAttribute(redis, OperatorContext.INITIAL_PARTITION_COUNT, 3);

    dag.addStream("ingen", input.outputPort, bucket.inputPort).setInline(true);
    dag.addStream("store", bucket.outputPort, redis.inputInd);
  }


}
