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
public class AggregationBenchmark implements StreamingApplication
{
  @Override
  public void populateDAG(DAG dag, Configuration conf)
  {
    LongArrayGenerator generator = dag.addOperator("Generator", LongArrayGenerator.class);
    LongAggregator aggregator = dag.addOperator("Aggregator", LongAggregator.class);

    dag.addStream("aggStream", generator.output, aggregator.input);
  }
}
