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

import junit.framework.Assert;

import org.junit.Test;

import com.datatorrent.lib.testbench.CollectorTestSink;
import com.lambdaworks.redis.RedisClient;
import com.lambdaworks.redis.RedisConnection;

/**
 *  Tests RedisBLPOPBytesInputOperator for ability to correctly read byte arrays from Redis.
 */
public class RedisBLPOPBytesInputOperatorTest
{
  private String[] testData = new String[]{"123", "abc", "def"};
  private String redisKey = "RedisBLPOPBytesInputOperatorTest";
  private String redisHost = "localhost";
  private int redisDb = 0;
  private int redisPort = 6379;

 
  private void generateData()
  {
    RedisClient client = new RedisClient(redisHost, redisPort);
    RedisConnection<String, String> connection = client.connect();
    connection.select(redisDb);
    connection.rpush(redisKey, testData);
    connection.close();
  }
  
  @SuppressWarnings({ "rawtypes", "unchecked" })
  @Test
  public void testInputOperator() throws InterruptedException {

    //Write data to Redis
    generateData();
    
    RedisBLPOPBytesInputOperator input = new RedisBLPOPBytesInputOperator();
    input.setHost(redisHost);
    input.setPort(redisPort);
    input.setDbIndex(redisDb);
    input.setRedisKeys(redisKey);
    
    CollectorTestSink<byte[]> sink = new CollectorTestSink<byte[]>();
    input.outputPort.setSink((CollectorTestSink)sink);
    
    
    input.setup(null);
    input.activate(null);
    

    input.beginWindow(0);

    //Read data from Redis
    long currentTimeMS = System.currentTimeMillis();
    
    // Allow maximum of 5 seconds for data to arrive from Redis
    while (System.currentTimeMillis() < currentTimeMS + 5000) {
      input.emitTuples();
      if (sink.collectedTuples.size() >= testData.length) break;
      Thread.sleep(100);
    }

    input.endWindow();
    input.deactivate();
    input.teardown();
    
    Assert.assertEquals("Number of emitted tuples", testData.length, sink.collectedTuples.size());
    for(int i=0; i< testData.length; i++)
    {
      Assert.assertEquals("value of "+ testData[i] + " == " + sink.collectedTuples.get(i), testData[i], new String(sink.collectedTuples.get(i)));
    }
  }

}
