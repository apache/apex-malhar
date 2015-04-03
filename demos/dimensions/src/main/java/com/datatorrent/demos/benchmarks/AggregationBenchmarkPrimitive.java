/*
 *  Copyright (c) 2012-2015 Malhar, Inc.
 *  All Rights Reserved.
 */

package com.datatorrent.demos.benchmarks;

import com.datatorrent.api.DAG;
import com.datatorrent.api.StreamingApplication;
import org.apache.hadoop.conf.Configuration;

/**
 *
 * @author Timothy Farkas: tim@datatorrent.com
 */
public class AggregationBenchmarkPrimitive implements StreamingApplication
{
  @Override
  public void populateDAG(DAG dag, Configuration conf)
  {
    LongArrayPrimitiveGenerator generator = dag.addOperator("Generator", LongArrayPrimitiveGenerator.class);
    LongAggregatorPrimitive aggregator = dag.addOperator("Aggregator", LongAggregatorPrimitive.class);

    dag.addStream("aggStream", generator.output, aggregator.input);
  }
}
