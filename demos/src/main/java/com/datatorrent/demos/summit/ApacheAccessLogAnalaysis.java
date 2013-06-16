/*
 *  Copyright (c) 2012 Malhar, Inc. All Rights Reserved.
 */
package com.datatorrent.demos.summit;

import java.net.URI;
import java.util.Map;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datatorrent.api.StreamingApplication;
import com.datatorrent.api.DAG;
import com.datatorrent.api.Context.OperatorContext;
import com.datatorrent.api.Operator.InputPort;
import com.datatorrent.contrib.ads_dimension.ApplicationRandomData;
import com.datatorrent.contrib.ads_dimension.ApplicationRandomData.AdsDimensionOperator;
import com.datatorrent.contrib.redis.RedisNumberAggregateOutputOperator;
import com.datatorrent.contrib.redis.RedisOutputOperator;
import com.datatorrent.lib.algo.TopN;
import com.datatorrent.lib.algo.TopNUnique;
import com.datatorrent.lib.io.ApacheGenRandomLogs;
import com.datatorrent.lib.io.ConsoleOutputOperator;
import com.datatorrent.lib.logs.ApacheVirtualLogParseOperator;
import com.datatorrent.lib.testbench.CompareFilterTuples;
import com.datatorrent.lib.testbench.CountOccurance;
import com.datatorrent.lib.testbench.HttpStatusFilter;
import com.datatorrent.lib.testbench.TopOccurance;
import com.datatorrent.lib.util.DimensionTimeBucketOperator;
import com.datatorrent.lib.util.DimensionTimeBucketSumOperator;
/**
 * @author Dinesh Prasad (dinesh@malhar-inc.com)
 */
public class ApacheAccessLogAnalaysis implements StreamingApplication
{
	private static final Logger LOG = LoggerFactory.getLogger(ApacheAccessLogAnalaysis.class);
	
	public static class TimeDimensionOperator extends DimensionTimeBucketSumOperator
  {
    @Override
    protected long extractTimeFromTuple(Map<String, Object> tuple)
    {
      return (Long)tuple.get("timestamp");
    }

  }
	
  private InputPort<Object> consoleOutput(DAG dag, String operatorName)
  {
    ConsoleOutputOperator operator = dag.addOperator(operatorName, new ConsoleOutputOperator());
    return operator.input;
  }
  
  public TimeDimensionOperator getPageDimensionTimeBucketSumOperator(String name, DAG dag)
  {
  	TimeDimensionOperator oper = dag.addOperator(name, TimeDimensionOperator.class);
    oper.addDimensionKeyName("item");
    oper.addValueKeyName("view");
    oper.setTimeBucketFlags(DimensionTimeBucketOperator.TIMEBUCKET_DAY | DimensionTimeBucketOperator.TIMEBUCKET_HOUR | DimensionTimeBucketOperator.TIMEBUCKET_MINUTE);
    return oper;
  }
  
  public InputPort<Map<String, Map<String, Number>>> getRedisOutput(String name, DAG dag, int dbIndex)
  {
    @SuppressWarnings("unchecked")
		RedisNumberAggregateOutputOperator<String, Map<String, Number>> oper = dag.addOperator(name, RedisNumberAggregateOutputOperator.class);
    oper.selectDatabase(dbIndex);
    return oper.input;
  }
  
	@Override
	public void populateDAG(DAG dag, Configuration conf)
	{
  	// Generate random apche logs
  	ApacheGenRandomLogs rand = dag.addOperator("rand", new ApacheGenRandomLogs());
  	
  	// parse log operator  
  	ApacheVirtualLogParseOperator parser = dag.addOperator("parser", new ApacheVirtualLogParseOperator());
  	dag.addStream("parserInput", rand.outport, parser.data).setInline(true);
  	
  	// count occurance operator  
  	CountOccurance<String> urlCounter = dag.addOperator("urlCounter", new CountOccurance<String>());
  	dag.addStream("urlStream", parser.outputUrl, urlCounter.inport);
  	dag.getMeta(urlCounter).getAttributes().attr(OperatorContext.APPLICATION_WINDOW_COUNT).set(2);
  	
    // url time dimension  
  	TimeDimensionOperator dimensionOperator = getPageDimensionTimeBucketSumOperator("Dimension", dag);
    dag.getMeta(dimensionOperator).getAttributes().attr(OperatorContext.APPLICATION_WINDOW_COUNT).set(10);
    dag.addStream("input_dimension", urlCounter.dimensionOut, dimensionOperator.in);
    dag.addStream("dimension_out", dimensionOperator.out,  getRedisOutput("redis", dag, 1)).setInline(true); 
  	
  	// format output for redix operator  
  	TopOccurance topOccur = dag.addOperator("topOccur", new TopOccurance());
  	topOccur.setN(20);
  	dag.addStream("topOccurStream", urlCounter.outport, topOccur.inport);
  	 
  	// redix output 
  	RedisOutputOperator<Integer, String> redis = dag.addOperator("topURLs", new RedisOutputOperator<Integer, String>());
  	redis.selectDatabase(2);
  	
  	// output to console      
    dag.addStream("rand_console", topOccur.outport, redis.input).setInline(true);
    
    // count ip occurance operator  
   	CountOccurance<String> ipCounter = dag.addOperator("ipCounter", new CountOccurance<String>());
   	dag.addStream("ipStream", parser.outputIPAddress, ipCounter.inport).setInline(true);
   	dag.getMeta(ipCounter).getAttributes().attr(OperatorContext.APPLICATION_WINDOW_COUNT).set(2);
   	
    // Top ip client counter  
    TopOccurance topIpOccur = dag.addOperator("topIpOccur", new TopOccurance());
    topIpOccur.setN(20);
    topIpOccur.setThreshHold(5);
  	dag.addStream("topIpOccurStream", ipCounter.outport, topIpOccur.inport).setInline(true);
  	
  	// output  ip counter    
  	RedisOutputOperator<Integer, String> redisIpCounter = dag.addOperator("redisIpCounter", new RedisOutputOperator<Integer, String>());
  	redisIpCounter.selectDatabase(3);
  	dag.addStream("topIpRedixStream", topIpOccur.outport,  redisIpCounter.input).setInline(true);
  	
    // count server name occurance operator  
   	CountOccurance<String> serverCounter = dag.addOperator("serverCounter", new CountOccurance<String>());
   	dag.addStream("serverStream", parser.outputServerName, serverCounter.inport).setInline(true);
   	dag.getMeta(serverCounter).getAttributes().attr(OperatorContext.APPLICATION_WINDOW_COUNT).set(2);
   	
   	// url time dimension  
   	TimeDimensionOperator serverDimensionOper = getPageDimensionTimeBucketSumOperator("serverDimensionOper", dag);
    dag.getMeta(serverDimensionOper).getAttributes().attr(OperatorContext.APPLICATION_WINDOW_COUNT).set(10);
    dag.addStream("server_dimension", serverCounter.dimensionOut, serverDimensionOper.in).setInline(true);
    dag.addStream("server_dimension_out", serverDimensionOper.out,  getRedisOutput("serverRedis", dag, 4)).setInline(true); 
    
    // output client more than 5 urls in sec
    RedisOutputOperator<Integer, String> redisgt5 = dag.addOperator("redisgt5", new RedisOutputOperator<Integer, String>());
    redisgt5.selectDatabase(5);
   	dag.addStream("redisgt5Stream", topIpOccur.gtThreshHold,  redisgt5.input).setInline(true);
   	
    // output client more than 5 urls in sec
    RedisOutputOperator<String, Integer> redisgt6 = dag.addOperator("redisgt6", new RedisOutputOperator<String, Integer>());
    redisgt6.selectDatabase(6);
   	dag.addStream("redisgt6Stream", urlCounter.total,  redisgt6.input).setInline(true);
   	
   	// get filter status operator 
    HttpStatusFilter urlHttpFilter = dag.addOperator("urlStatusCheck", new HttpStatusFilter());
    urlHttpFilter.setFilterStatus("404");
   	dag.addStream("urlStatusCheckStream", parser.outUrlStatus, urlHttpFilter.inport).setInline(true);
  	RedisOutputOperator<Integer, String> redisgt7 = dag.addOperator("redisgt7", new RedisOutputOperator<Integer, String>());
    redisgt7.selectDatabase(7);
   	dag.addStream("redisgt7Stream", urlHttpFilter.outport,  redisgt7.input).setInline(true);
   	//dag.addStream("redisgt7Stream", urlHttpFilter.outport,  consoleOutput(dag, "console")).setInline(true);
   	
   	// get ip client status operator  
   	HttpStatusFilter ipHttpFilter = dag.addOperator("ipHttpFilter", new HttpStatusFilter());
   	ipHttpFilter.setFilterStatus("404");
   	dag.addStream("ipHttpFilterStream", parser.outIpStatus, ipHttpFilter.inport).setInline(true);
  	RedisOutputOperator<Integer, String> redisgt8 = dag.addOperator("redisgt8", new RedisOutputOperator<Integer, String>());
    redisgt8.selectDatabase(8);
   	dag.addStream("redisgt8Stream", ipHttpFilter.outport,  redisgt8.input).setInline(true);
   	//dag.addStream("redisgt8Stream", parser.outIpStatus,  consoleOutput(dag, "console")).setInline(true);
   	
   	// client data usage 
   	CompareFilterTuples<String> clientDataFilter =  dag.addOperator("clientDataFilter", new CompareFilterTuples<String>());
   	clientDataFilter.setCompareType(1);
   	clientDataFilter.setValue(1000);
   	dag.addStream("clientDataFilterStream", parser.clientDataUsage, clientDataFilter.inport).setInline(true);
   	RedisOutputOperator<Integer, String> redisgt9 = dag.addOperator("redisgt9", new RedisOutputOperator<Integer, String>());
    redisgt9.selectDatabase(9);
   	dag.addStream("redisgt9Stream", clientDataFilter.redisport,  redisgt9.input).setInline(true);
   	//dag.addStream("redisgt9Stream", clientDataFilter.redisport,  consoleOutput(dag, "console")).setInline(true);
  }
}
