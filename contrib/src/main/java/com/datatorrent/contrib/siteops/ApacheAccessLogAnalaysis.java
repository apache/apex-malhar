/**
 * Copyright (C) 2015 DataTorrent, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.datatorrent.contrib.siteops;

import java.util.Map;

import org.apache.hadoop.conf.Configuration;

import com.datatorrent.lib.io.ApacheGenRandomLogs;
import com.datatorrent.lib.logs.ApacheVirtualLogParseOperator;
import com.datatorrent.lib.math.Sum;
import com.datatorrent.lib.testbench.*;
import com.datatorrent.lib.util.AbstractDimensionTimeBucketOperator;
import com.datatorrent.lib.util.DimensionTimeBucketSumOperator;

import com.datatorrent.contrib.redis.RedisMapOutputOperator;
import com.datatorrent.contrib.redis.RedisNumberSummationMapOutputOperator;

import com.datatorrent.api.Context.OperatorContext;
import com.datatorrent.api.DAG;
import com.datatorrent.api.DAG.Locality;
import com.datatorrent.api.Operator.InputPort;
import com.datatorrent.api.StreamingApplication;
import com.datatorrent.api.annotation.ApplicationAnnotation;

/**
 * <p>ApacheAccessLogAnalaysis class.</p>
 *
 * @since 0.3.2
 * @deprecated please use logstream instead
 */
@ApplicationAnnotation(name = "SiteOps")
@Deprecated
public class ApacheAccessLogAnalaysis implements StreamingApplication
{
  public static class TimeDimensionOperator extends DimensionTimeBucketSumOperator
  {
    @Override
    protected long extractTimeFromTuple(Map<String, Object> tuple)
    {
      return (Long)tuple.get("timestamp");
    }

  }

  public TimeDimensionOperator getPageDimensionTimeBucketSumOperator(String name, DAG dag)
  {
    TimeDimensionOperator oper = dag.addOperator(name, TimeDimensionOperator.class);
    oper.addDimensionKeyName("item");
    oper.addValueKeyName("view");
    oper.setTimeBucketFlags(AbstractDimensionTimeBucketOperator.TIMEBUCKET_DAY | AbstractDimensionTimeBucketOperator.TIMEBUCKET_HOUR | AbstractDimensionTimeBucketOperator.TIMEBUCKET_MINUTE);
    return oper;
  }

  public InputPort<Map<String, Map<String, Number>>> getRedisOutput(String name, DAG dag, int dbIndex)
  {
    @SuppressWarnings("unchecked")
    RedisNumberSummationMapOutputOperator<String, Map<String, Number>> oper = dag.addOperator(name, RedisNumberSummationMapOutputOperator.class);
    oper.getStore().setDbIndex(dbIndex);
    return oper.input;
  }

  @Override
  public void populateDAG(DAG dag, Configuration conf)
  {
    // set app name
    dag.setAttribute(DAG.APPLICATION_NAME, "SiteOperationsApplication");

    // Generate random apche logs
    ApacheGenRandomLogs rand = dag.addOperator("rand", new ApacheGenRandomLogs());

    // parse log operator
    ApacheVirtualLogParseOperator parser = dag.addOperator("parser", new ApacheVirtualLogParseOperator());
    dag.addStream("parserInput", rand.outport, parser.data).setLocality(Locality.CONTAINER_LOCAL);

    // count occurance operator
    CountOccurance<String> urlCounter = dag.addOperator("urlCounter", new CountOccurance<String>());
    dag.addStream("urlStream", parser.outputUrl, urlCounter.inport);
    dag.getMeta(urlCounter).getAttributes().put(OperatorContext.APPLICATION_WINDOW_COUNT, 2);

    // url time dimension
    TimeDimensionOperator dimensionOperator = getPageDimensionTimeBucketSumOperator("Dimension", dag);
    dag.getMeta(dimensionOperator).getAttributes().put(OperatorContext.APPLICATION_WINDOW_COUNT, 10);
    dag.addStream("input_dimension", urlCounter.dimensionOut, dimensionOperator.in);
    dag.addStream("dimension_out", dimensionOperator.out, getRedisOutput("redisapachelog1", dag, 1)).setLocality(Locality.CONTAINER_LOCAL);

    // format output for redix operator
    TopOccurrence topOccur = dag.addOperator("topOccur", new TopOccurrence());
    topOccur.setN(10);
    dag.addStream("topOccurStream", urlCounter.outport, topOccur.inport);

    // redis output
    RedisMapOutputOperator<Integer, String> redis = dag.addOperator("redisapachelog2", new RedisMapOutputOperator<Integer, String>());
    redis.getStore().setDbIndex(2);

    // output to console
    dag.addStream("rand_console", topOccur.outport, redis.input).setLocality(Locality.CONTAINER_LOCAL);

    // count server name occurance operator
    CountOccurance<String> serverCounter = dag.addOperator("serverCounter", new CountOccurance<String>());
    dag.addStream("serverStream", parser.outputServerName, serverCounter.inport).setLocality(Locality.CONTAINER_LOCAL);
    dag.getMeta(serverCounter).getAttributes().put(OperatorContext.APPLICATION_WINDOW_COUNT, 2);

    // url time dimension
    TimeDimensionOperator serverDimensionOper = getPageDimensionTimeBucketSumOperator("serverDimensionOper", dag);
    dag.getMeta(serverDimensionOper).getAttributes().put(OperatorContext.APPLICATION_WINDOW_COUNT, 10);
    dag.addStream("server_dimension", serverCounter.dimensionOut, serverDimensionOper.in).setLocality(Locality.CONTAINER_LOCAL);
    dag.addStream("server_dimension_out", serverDimensionOper.out, getRedisOutput("redisapachelog4", dag, 4)).setLocality(Locality.CONTAINER_LOCAL);


    // count server name occurance operator
    CountOccurance<String> serverCounter1 = dag.addOperator("serverCounter1", new CountOccurance<String>());
    dag.addStream("serverStream1", parser.outputServerName1, serverCounter1.inport).setLocality(Locality.CONTAINER_LOCAL);
    dag.getMeta(serverCounter1).getAttributes().put(OperatorContext.APPLICATION_WINDOW_COUNT, 2);

    // format output for redix operator
    TopOccurrence serverTop = dag.addOperator("serverTop", new TopOccurrence());
    serverTop.setN(10);
    dag.addStream("serverTopStream", serverCounter1.outport, serverTop.inport);
    //dag.addStream("redisgt8Stream", serverTop.outport,  consoleOutput(dag, "console")).setInline(true);

    // redix output
    RedisMapOutputOperator<Integer, String> redis10 = dag.addOperator("redisapachelog10", new RedisMapOutputOperator<Integer, String>());
    redis10.getStore().setDbIndex(10);
    dag.addStream("rand_console10", serverTop.outport, redis10.input).setLocality(Locality.CONTAINER_LOCAL);

    // count ip occurance operator
    CountOccurance<String> ipCounter = dag.addOperator("ipCounter", new CountOccurance<String>());
    dag.addStream("ipStream", parser.outputIPAddress, ipCounter.inport).setLocality(Locality.CONTAINER_LOCAL);
    dag.getMeta(ipCounter).getAttributes().put(OperatorContext.APPLICATION_WINDOW_COUNT, 2);

    // Top ip client counter
    TopOccurrence topIpOccur = dag.addOperator("topIpOccur", new TopOccurrence());
    topIpOccur.setN(10);
    topIpOccur.setThreshold(1000);
    dag.addStream("topIpOccurStream", ipCounter.outport, topIpOccur.inport).setLocality(Locality.CONTAINER_LOCAL);

    // output  ip counter
    RedisMapOutputOperator<Integer, String> redisIpCounter = dag.addOperator("redisapachelog3", new RedisMapOutputOperator<Integer, String>());
    redisIpCounter.getStore().setDbIndex(3);
    dag.addStream("topIpRedixStream", topIpOccur.outport, redisIpCounter.input).setLocality(Locality.CONTAINER_LOCAL);

    // output client more than 5 urls in sec
    RedisMapOutputOperator<Integer, String> redisgt5 = dag.addOperator("redisapachelog5", new RedisMapOutputOperator<Integer, String>());
    redisgt5.getStore().setDbIndex(5);
    dag.addStream("redisgt5Stream", topIpOccur.gtThreshold, redisgt5.input).setLocality(Locality.CONTAINER_LOCAL);

    // get filter status operator
    HttpStatusFilter urlHttpFilter = dag.addOperator("urlStatusCheck", new HttpStatusFilter());
    urlHttpFilter.setFilterStatus("404");
    dag.getMeta(urlHttpFilter).getAttributes().put(OperatorContext.APPLICATION_WINDOW_COUNT, 2);
    dag.addStream("urlStatusCheckStream", parser.outUrlStatus, urlHttpFilter.inport).setLocality(Locality.CONTAINER_LOCAL);
    TopOccurrence topUrlStatus = dag.addOperator("topUrlStatus", new TopOccurrence());
    topUrlStatus.setN(10);
    dag.addStream("topUrlStatusStream", urlHttpFilter.outport, topUrlStatus.inport).setLocality(Locality.CONTAINER_LOCAL);
    // dag.addStream("testconsole", topUrlStatus.outport,  consoleOutput(dag, "console")).setInline(true);
    RedisMapOutputOperator<Integer, String> redisgt7 = dag.addOperator("redisapachelog7", new RedisMapOutputOperator<Integer, String>());
    redisgt7.getStore().setDbIndex(7);
    dag.addStream("redisgt7Stream", topUrlStatus.outport, redisgt7.input).setLocality(Locality.CONTAINER_LOCAL);

    // client data usage
    Sum<Integer> totalOper = dag.addOperator("totaloper", new Sum<Integer>());
    dag.getMeta(totalOper).getAttributes().put(OperatorContext.APPLICATION_WINDOW_COUNT, 2);
    dag.addStream("clientDataFilterStream", parser.clientDataUsage, totalOper.data).setLocality(Locality.CONTAINER_LOCAL);
    RedisMapOutputOperator<Integer, Integer> redisgt9 = dag.addOperator("redisapachelog9", new RedisMapOutputOperator<Integer, Integer>());
    redisgt9.getStore().setDbIndex(9);
    dag.addStream("redisgt9Stream", totalOper.redisport, redisgt9.input).setLocality(Locality.CONTAINER_LOCAL);
    //dag.addStream("redisgt9Stream", clientDataFilter.redisport,  consoleOutput(dag, "console")).setInline(true);  */

    // get filter status operator
    HttpStatusFilter serverHttpFilter = dag.addOperator("serverHttpFilter", new HttpStatusFilter());
    serverHttpFilter.setFilterStatus("404");
    dag.getMeta(serverHttpFilter).getAttributes().put(OperatorContext.APPLICATION_WINDOW_COUNT, 2);
    dag.addStream("serverHttpFilterStream", parser.outServerStatus, serverHttpFilter.inport).setLocality(Locality.CONTAINER_LOCAL);
    TopOccurrence serverTop404 = dag.addOperator("serverTop404", new TopOccurrence());
    serverTop404.setN(10);
    dag.addStream("serverTop404Stream", serverHttpFilter.outport, serverTop404.inport).setLocality(Locality.CONTAINER_LOCAL);
    RedisMapOutputOperator<Integer, String> redisgt8 = dag.addOperator("redisapachelog8", new RedisMapOutputOperator<Integer, String>());
    redisgt8.getStore().setDbIndex(8);
    dag.addStream("redisgt8Stream", serverTop404.outport, redisgt8.input).setLocality(Locality.CONTAINER_LOCAL);

    // data collect for each
    KeyValSum ipDataCollect = dag.addOperator("ipDataCollect", new KeyValSum());
    dag.getMeta(ipDataCollect).getAttributes().put(OperatorContext.APPLICATION_WINDOW_COUNT, 2);
    dag.addStream("ipDataCollectStream", parser.outputBytes, ipDataCollect.inport).setLocality(Locality.CONTAINER_LOCAL);
    TopOccurrence topIpData = dag.addOperator("topIpData", new TopOccurrence());
    topIpData.setN(10);
    dag.addStream("topIpDataStream", ipDataCollect.outport, topIpData.inport).setLocality(Locality.CONTAINER_LOCAL);
    //dag.addStream("consoletest", topIpData.outport,  consoleOutput(dag, "console")).setInline(true);
    RedisMapOutputOperator<Integer, String> redisgt6 = dag.addOperator("redisapachelog6", new RedisMapOutputOperator<Integer, String>());
    redisgt6.getStore().setDbIndex(6);
    dag.addStream("redisgt6Stream", topIpData.outport, redisgt6.input).setLocality(Locality.CONTAINER_LOCAL);

    // total view count
    RedisSumOper totalViewcount = dag.addOperator("totalViewcount", new RedisSumOper());
    dag.getMeta(totalViewcount).getAttributes().put(OperatorContext.APPLICATION_WINDOW_COUNT, 2);
    dag.addStream("totalViewcountStream", parser.viewCount, totalViewcount.inport).setLocality(Locality.CONTAINER_LOCAL);
    //dag.addStream("consoletest", totalViewcount.sumInteger,  consoleOutput(dag, "console")).setInline(true);
    RedisMapOutputOperator<Integer, Integer> redisgt11 = dag.addOperator("redisapachelog11", new RedisMapOutputOperator<Integer, Integer>());
    redisgt11.getStore().setDbIndex(11);
    dag.addStream("redisgtlog11Stream", totalViewcount.outport, redisgt11.input).setLocality(Locality.CONTAINER_LOCAL);
  }

}
