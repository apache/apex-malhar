/*
 * Copyright (c) 2013 Malhar Inc. ALL Rights Reserved.
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
package com.datatorrent.contrib.ads_dimension;

import com.datatorrent.contrib.redis.RedisNumberAggregateOutputOperator;
import com.datatorrent.lib.io.ConsoleOutputOperator;
import com.datatorrent.lib.util.DimensionTimeBucketOperator;
import com.datatorrent.lib.util.DimensionTimeBucketSumOperator;
import com.datatorrent.api.StreamingApplication;
import com.datatorrent.api.Context.OperatorContext;
import com.datatorrent.api.DAG;
import com.datatorrent.api.Operator.InputPort;
import java.util.Map;
import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ApplicationRandomData implements StreamingApplication
{
  private static final Logger LOG = LoggerFactory.getLogger(ApplicationRandomData.class);

  public static class AdsDimensionOperator extends DimensionTimeBucketSumOperator
  {
    @Override
    protected long extractTimeFromTuple(Map<String, Object> tuple)
    {
      return (Long)tuple.get("timestamp");
    }

  }

  public AdsDimensionLogInputOperator getAdsDimensionInputOperator(String name, DAG dag)
  {
    AdsDimensionRandomInputOperator oper = dag.addOperator(name, AdsDimensionRandomInputOperator.class);
    return oper;
  }

  public AdsDimensionDimensionOperator getDimensionTimeBucketSumOperator(String name, DAG dag)
  {
    AdsDimensionDimensionOperator oper = dag.addOperator(name, AdsDimensionDimensionOperator.class);
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
  public void populateDAG(DAG dag, Configuration conf)
  {
    DAG dag = new DAG(conf);
    AdsDimensionLogInputOperator adsDimensionInputOperator = getAdsDimensionInputOperator("AdsDimensionInput", dag);
    AdsDimensionDimensionOperator dimensionOperator = getDimensionTimeBucketSumOperator("Dimension", dag);
    dag.getMeta(dimensionOperator).getAttributes().attr(OperatorContext.APPLICATION_WINDOW_COUNT).set(10);

    dag.addStream("input_dimension", adsDimensionInputOperator.outputPort, dimensionOperator.in);
    dag.addStream("dimension_out", dimensionOperator.out, /*getConsole("Console", dag, "Console"),*/ getRedisOutput("redis", dag));
  }

}
