/*
 *  Copyright (c) 2012-2013 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.malhartech.demos.ads_dimension;

import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.malhartech.api.ApplicationFactory;
import com.malhartech.api.Context.OperatorContext;
import com.malhartech.api.DAG;
import com.malhartech.api.Operator.InputPort;
import com.malhartech.contrib.redis.RedisNumberAggregateOutputOperator;
import com.malhartech.lib.io.ConsoleOutputOperator;
import com.malhartech.lib.util.DimensionTimeBucketOperator;
import com.malhartech.lib.util.DimensionTimeBucketSumOperator;

/**
 * Yahoo! Finance application demo. <p>
 *
 * Get Yahoo finance feed and calculate minute price range, minute volume, simple moving average of 5 minutes.
 */
public class Application implements ApplicationFactory
{
  private static final Logger LOG = LoggerFactory.getLogger(Application.class);

  public static class AdsDimensionOperator extends DimensionTimeBucketSumOperator
  {
    @Override
    protected long extractTimeFromTuple(Map<String, Object> tuple)
    {
      String ts = (String)tuple.get("e");
      return Long.parseLong(ts) * 1000;
    }

  }

  public AdsDimensionLogInputOperator getAdsDimensionInputOperator(String name, DAG dag)
  {
    AdsDimensionLogInputOperator oper = dag.addOperator(name, AdsDimensionLogInputOperator.class);
    return oper;
  }

  public AdsDimensionOperator getDimensionTimeBucketSumOperator(String name, DAG dag)
  {
    AdsDimensionOperator oper = dag.addOperator(name, AdsDimensionOperator.class);
    oper.addDimensionKeyName("u:ptnr");
    oper.addDimensionKeyName("d:offer_source_id");
    oper.addDimensionKeyName("d:adunit_name");
    oper.addValueKeyName("d:cost");
    oper.setTimeBucketFlags(DimensionTimeBucketOperator.TIMEBUCKET_DAY | DimensionTimeBucketOperator.TIMEBUCKET_HOUR | DimensionTimeBucketOperator.TIMEBUCKET_MINUTE);
    return oper;
  }

  public InputPort<Object> getConsole(String name, DAG dag, String prefix)
  {
    ConsoleOutputOperator oper = dag.addOperator(name, ConsoleOutputOperator.class);
    oper.setStringFormat(prefix + ": %s");
    return oper.input;
  }

  public InputPort<Map<String, Map<String, Number>>> getRedisOutput(String name, DAG dag)
  {
    RedisNumberAggregateOutputOperator<String, Map<String, Number>> oper = dag.addOperator(name, RedisNumberAggregateOutputOperator.class);
    return oper.input;
    //DevNull<Map<String, Map<String, Number>>> oper = dag.addOperator(name, DevNull.class);
    //return oper.data;
  }

  @Override
  public void getApplication(DAG dag, Configuration conf)
  {
    DAG dag = new DAG(conf);
    AdsDimensionLogInputOperator adsDimensionInputOperator = getAdsDimensionInputOperator("AdsDimensionInput", dag);
    AdsDimensionOperator dimensionOperator = getDimensionTimeBucketSumOperator("Dimension", dag);
    dag.getMeta(dimensionOperator).getAttributes().attr(OperatorContext.APPLICATION_WINDOW_COUNT).set(10);

    dag.addStream("input_dimension", adsDimensionInputOperator.outputPort, dimensionOperator.in);
    dag.addStream("dimension_out", dimensionOperator.out, /*getConsole("Console", dag, "Console"),*/ getRedisOutput("redis", dag));

  }

}
