/*
* Copyright (c) 2013 DataTorrent, Inc. ALL Rights Reserved.
*
* Licensed under the Apache License, Version 2.0 (the "License");
* you may not use this file except in compliance with the License.
* You may obtain a copy of the License at
*
* http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing, software
* distributed under the License is distributed on an "AS IS" BASIS,
* WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
* See the License for the specific language governing permissions and
* limitations under the License.
*/
package com.datatorrent.contrib.redis;

import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datatorrent.api.DAG;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.api.InputOperator;
import com.datatorrent.api.Context.OperatorContext;
import com.datatorrent.api.StreamingApplication;
import com.datatorrent.contrib.redis.RedisOutputOperator;
import com.datatorrent.lib.util.KeyHashValPair;
import com.datatorrent.lib.util.KeyValPair;

public class RedisTestApplication implements StreamingApplication 
{
  
  static class RandomNumberGenerator implements InputOperator
  {
    private static final Logger LOG = LoggerFactory.getLogger(RandomNumberGenerator.class);
    public final transient DefaultOutputPort<KeyHashValPair<Integer, Integer>> output = new DefaultOutputPort<KeyHashValPair<Integer, Integer>>();

    @Override
    public void emitTuples()
    {
      ++count;
      output.emit(new KeyHashValPair<Integer, Integer>(count, count));
    }

    @Override
    public void beginWindow(long windowId)
    {
    }

    @Override
    public void endWindow()
    {
    }

    @Override
    public void setup(OperatorContext context)
    {
    }

    @Override
    public void teardown()
    {
    }

    int count;
  }
  
  
  public static final Logger logger = LoggerFactory.getLogger(RedisTestApplication.class);

  @Override
  public void populateDAG(DAG dag, Configuration conf)
  {
    RandomNumberGenerator rng = dag.addOperator("random", new RandomNumberGenerator());
    RedisOutputOperator<Integer, Integer> redis = dag.addOperator("redis", new RedisOutputOperator<Integer,Integer>());
    String connectionList = conf.get("redsitestapplication.hostlist","localhost,6379,1|localhost,6379,2|localhost,6379,3");
    int partitions = conf.getInt("redsitestapplication.partition",3);
    redis.setConnectionList(connectionList);
    redis.setKeyExpiryTime(60);
    dag.setAttribute(redis, OperatorContext.INITIAL_PARTITION_COUNT, partitions);
    dag.addStream("stream", rng.output, redis.inputInd);    
    
  }
  
  

}
