/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.apex.malhar.stream.sample;

import java.util.Map;
import java.util.concurrent.Callable;

import org.junit.Assert;
import org.junit.Test;

import org.apache.apex.malhar.lib.util.KeyValPair;
import org.apache.hadoop.conf.Configuration;

import com.datatorrent.api.LocalMode;
import com.datatorrent.stram.StramLocalCluster;

/**
 * Testing the TwitterAutoComplete Application. In order to run this test, you need to create an app
 * at https://apps.twitter.com, then generate your consumer and access keys and tokens, and set the following properties
 * for the application before running it:
 * Your application consumer key,
 * Your application consumer secret,
 * Your twitter access token, and
 * Your twitter access token secret.
 */
public class WindowedWordCountTest
{
  @Test
  public void WindowedWordCountTest() throws Exception
  {
    LocalMode lma = LocalMode.newInstance();
    Configuration conf = new Configuration(false);
    conf.set("dt.application.WindowedWordCount.operator.console.silent", "true");
    lma.prepareDAG(new WindowedWordCount(), conf);
    LocalMode.Controller lc = lma.getController();
    ((StramLocalCluster)lc).setExitCondition(new Callable<Boolean>()
    {
      @Override
      public Boolean call() throws Exception
      {
        return WindowedWordCount.Collector.isDone();
      }
    });

    lc.run(60000);

    Assert.assertEquals(127, countSum(WindowedWordCount.Collector.getResult()));
    Assert.assertEquals(28, countSumWord(WindowedWordCount.Collector.getResult(), "word2"));
    Assert.assertEquals(7, countSumWord(WindowedWordCount.Collector.getResult(), "error"));
    Assert.assertEquals(21, countSumWord(WindowedWordCount.Collector.getResult(), "word9"));
    Assert.assertEquals(1, countSumWord(WindowedWordCount.Collector.getResult(), "bye"));
  }

  public long countSum(Map<KeyValPair<Long, String>, Long> map)
  {
    long sum = 0;
    for (long count : map.values()) {
      sum += count;
    }
    return sum;
  }

  public long countSumWord(Map<KeyValPair<Long, String>, Long> map, String word)
  {
    long sum = 0;
    for (Map.Entry<KeyValPair<Long, String>, Long> entry : map.entrySet()) {
      if (entry.getKey().getValue().equals(word)) {
        sum += entry.getValue();
      }
    }
    return sum;
  }

}

