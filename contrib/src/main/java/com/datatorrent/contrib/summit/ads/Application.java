/*
 *  Copyright (c) 2012-2013 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.datatorrent.contrib.summit.ads;

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
    InputGenerator in = dag.addOperator("inputgen", InputGenerator.class);
    dag.setOutputPortAttribute(in.outputPort, PortContext.QUEUE_CAPACITY, 32 * 1024);
    BucketOperator bop = dag.addOperator("bucket", BucketOperator.class);
    dag.setInputPortAttribute(bop.inputPort, PortContext.QUEUE_CAPACITY, 32 * 1024);
    dag.setOutputPortAttribute(bop.outputPort, PortContext.QUEUE_CAPACITY, 32 * 1024);

    RedisNumberAggregateOutputOperator<AggrKey, Map<String, MutableDouble>> redis = dag.addOperator("redis", RedisNumberAggregateOutputOperator.class);
    dag.setInputPortAttribute(redis.input, PortContext.QUEUE_CAPACITY, 32 * 1024);

    dag.addStream("ingen", in.outputPort, bop.inputPort).setInline(true);
    dag.addStream("store", bop.outputPort, redis.input);

  }


}
