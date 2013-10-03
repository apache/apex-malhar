/*
 * Copyright (c) 2013 DataTorrent, Inc. ALL Rights Reserved.
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
package com.datatorrent.contrib.redis;

import java.util.ArrayList;

import junit.framework.Assert;

import org.junit.Test;

import com.datatorrent.contrib.redis.AbstractRedisListInputOperator.RedisReadStrategy;
import com.lambdaworks.redis.RedisClient;
import com.lambdaworks.redis.RedisConnection;
import com.lambdaworks.redis.codec.RedisCodec;
import com.lambdaworks.redis.codec.Utf8StringCodec;

/**
 *  Tests AbstractRedisListInputOperator for ability to correctly read strings from Redis.
 */
public class AbstractRedisListInputOperatorTest
{
  private ArrayList<String> resultList = new ArrayList<String>();
  private String[] testData = new String[]{"123", "abc", "def"};
  private String redisKey = "RedisBLPOPBytesInputOperatorTest";
  private String redisHost = "localhost";
  private int redisDb = 0;
  private int redisPort = 6379;

  public class RedisBLPOPStringInputOperatorTest extends AbstractRedisListInputOperator<String, String>
  {

    @Override
    public RedisReadStrategy<String, String> getReadStrategy()
    {
      return new AbstractRedisListInputOperator.RedisBLPOP<String,String>();
    }
    
    @Override
    public RedisCodec<String,String> getCodec()
    {
      return new Utf8StringCodec();
    }
    
    @Override
    public void emitTuple(String message)
    {
      resultList.add(message);
    }
  }
  
  private void generateData()
  {
    RedisClient client = new RedisClient(redisHost, redisPort);
    RedisConnection<String, String> connection = client.connect();
    connection.select(redisDb);
    connection.rpush(redisKey, testData);
    connection.close();
  }
  
  @Test
  public void testInputOperator() throws InterruptedException {

    //Write data to Redis
    generateData();
    
    RedisBLPOPStringInputOperatorTest input = new RedisBLPOPStringInputOperatorTest();
    input.setHost(redisHost);
    input.setPort(redisPort);
    input.setDbIndex(redisDb);
    input.setRedisKeys(redisKey);
    

    input.setup(null);
    input.activate(null);
    

    input.beginWindow(0);

    //Read data from Redis
    long currentTimeMS = System.currentTimeMillis();
    
    // Allow maximum of 5 seconds for data to arrive from Redis
    while (System.currentTimeMillis() < currentTimeMS + 5000) {
      input.emitTuples();
      if (resultList.size() >= testData.length) break;
      Thread.sleep(100);
    }

    input.endWindow();
    input.deactivate();
    input.teardown();
    
    Assert.assertEquals("Number of emitted tuples", testData.length, resultList.size());
    for(int i=0; i< testData.length; i++)
    {
      Assert.assertEquals("value of "+ testData[i] + " == " + resultList.get(i), testData[i], resultList.get(i));
    }
  }

}
