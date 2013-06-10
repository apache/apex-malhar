/*
 *  Copyright (c) 2012 Malhar, Inc. All Rights Reserved.
 */
package com.malhartech.demos.summit;

import java.net.URI;
import java.util.Map;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.malhartech.api.ApplicationFactory;
import com.malhartech.api.DAG;
import com.malhartech.api.Context.OperatorContext;
import com.malhartech.api.Operator.InputPort;
import com.malhartech.contrib.ads_dimension.ApplicationRandomData;
import com.malhartech.contrib.ads_dimension.ApplicationRandomData.AdsDimensionOperator;
import com.malhartech.contrib.redis.RedisNumberAggregateOutputOperator;
import com.malhartech.contrib.redis.RedisOutputOperator;
import com.malhartech.lib.algo.TopN;
import com.malhartech.lib.algo.TopNUnique;
import com.malhartech.lib.io.ConsoleOutputOperator;
import com.malhartech.lib.io.ApacheGenRandomLogs;
import com.malhartech.lib.io.PubSubWebSocketOutputOperator;
import com.malhartech.lib.logs.ApacheVirtualLogParseOperator;
import com.malhartech.lib.testbench.CompareFilterTuples;
import com.malhartech.lib.testbench.CountOccurance;
import com.malhartech.lib.testbench.RandomEventGenerator;
import com.malhartech.lib.testbench.TopOccurance;
import com.malhartech.lib.util.DimensionTimeBucketOperator;
import com.malhartech.lib.util.DimensionTimeBucketSumOperator;
/**
 * @author Dinesh Prasad (dinesh@malhar-inc.com)
 */
public class ApacheAccessLogAnalaysis implements ApplicationFactory
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
  public void getApplication(DAG dag, Configuration conf)
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
    dag.addStream("dimension_out", dimensionOperator.out,  getRedisOutput("redis", dag, 0)); 
  	
  	// format output for redix operator  
  	TopOccurance topOccur = dag.addOperator("topOccur", new TopOccurance());
  	topOccur.setN(20);
  	dag.addStream("topOccurStream", urlCounter.outport, topOccur.inport);
  	 
  	// redix output 
  	RedisOutputOperator<Integer, String> redis = dag.addOperator("topURLs", new RedisOutputOperator<Integer, String>());
  	redis.selectDatabase(1);
  	
  	// output to console      
    dag.addStream("rand_console", topOccur.outport, redis.input).setInline(true);
    
    // count ip occurance operator  
   	CountOccurance<String> ipCounter = dag.addOperator("ipCounter", new CountOccurance<String>());
   	dag.addStream("ipStream", parser.outputIPAddress, ipCounter.inport);
   	dag.getMeta(ipCounter).getAttributes().attr(OperatorContext.APPLICATION_WINDOW_COUNT).set(2);
   	
    // Top ip client counter  
    TopOccurance topIpOccur = dag.addOperator("topIpOccur", new TopOccurance());
    topIpOccur.setN(20);
  	dag.addStream("topIpOccurStream", ipCounter.outport, topIpOccur.inport);
  	
  	// output  ip counter    
  	RedisOutputOperator<Integer, String> redisIpCounter = dag.addOperator("redisIpCounter", new RedisOutputOperator<Integer, String>());
  	redisIpCounter.selectDatabase(2);
  	dag.addStream("topIpRedixStream", topIpOccur.outport, /*consoleOutput(dag, "topIpRedixOper")*/ redisIpCounter.input).setInline(true);
  	
    // count server name occurance operator  
   	CountOccurance<String> serverCounter = dag.addOperator("serverCounter", new CountOccurance<String>());
   	dag.addStream("serverStream", parser.outputServerName, serverCounter.inport);
   	dag.getMeta(serverCounter).getAttributes().attr(OperatorContext.APPLICATION_WINDOW_COUNT).set(2);
   	
   	// url time dimension  
   	TimeDimensionOperator serverDimensionOper = getPageDimensionTimeBucketSumOperator("serverDimensionOper", dag);
    dag.getMeta(serverDimensionOper).getAttributes().attr(OperatorContext.APPLICATION_WINDOW_COUNT).set(10);
    dag.addStream("server_dimension", serverCounter.dimensionOut, serverDimensionOper.in);
    dag.addStream("server_dimension_out", serverDimensionOper.out,  getRedisOutput("serverRedis", dag, 3) /*consoleOutput(dag, "topIpRedixOper")*/); 
  }
}
