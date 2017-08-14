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

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;

import org.junit.Assert;
import org.junit.Test;

import org.apache.apex.malhar.lib.function.Function;
import org.apache.apex.malhar.lib.util.KeyValPair;
import org.apache.apex.malhar.lib.window.TriggerOption;
import org.apache.apex.malhar.lib.window.Tuple;
import org.apache.apex.malhar.lib.window.WindowOption;
import org.apache.apex.malhar.stream.api.impl.StreamFactory;

/**
 * A embedded application test without creating Streaming Application
 */
public class LocalTestWithoutStreamApplication
{
  @Test
  @SuppressWarnings("unchecked")
  public void testNonStreamApplicationWordcount() throws Exception
  {

    TupleCollector<Tuple.WindowedTuple<KeyValPair<String, Long>>> collector = new TupleCollector<>();
    collector.id = "testNonStreamApplicationWordcount";
    final Map<String, Long> expected = new HashMap<>();
    expected.put("error", 2L);
    expected.put("word1", 4L);
    expected.put("word2", 8L);
    expected.put("word3", 4L);
    expected.put("word4", 4L);
    expected.put("word5", 4L);
    expected.put("word7", 4L);
    expected.put("word9", 6L);

    Callable<Boolean> exitCondition = new Callable<Boolean>()
    {
      @Override
      public Boolean call() throws Exception
      {
        if (!TupleCollector.results.containsKey("testNonStreamApplicationWordcount") || TupleCollector.results.get("testNonStreamApplicationWordcount").isEmpty()) {
          return false;
        }
        Map<String, Long> data = new HashMap<>();
        for (Tuple.TimestampedTuple<KeyValPair<String, Long>> entry :
            (List<Tuple.TimestampedTuple<KeyValPair<String, Long>>>)TupleCollector.results.get("testNonStreamApplicationWordcount")) {
          data.put(entry.getValue().getKey(), entry.getValue().getValue());
        }
        return data.size() >= 8 && expected.equals(data);
      }
    };


    StreamFactory.fromFolder("./src/test/resources/data")
        .flatMap(new Function.FlatMapFunction<String, String>()
        {
          @Override
          public Iterable<String> f(String input)
          {
            return Arrays.asList(input.split(" "));
          }
        })
        .window(new WindowOption.GlobalWindow(), new TriggerOption().accumulatingFiredPanes().withEarlyFiringsAtEvery(1))
        .countByKey(new Function.ToKeyValue<String, String, Long>()
        {
          @Override
          public Tuple<KeyValPair<String, Long>> f(String input)
          {
            return new Tuple.PlainTuple(new KeyValPair<>(input, 1L));
          }
        }).addOperator(collector, collector.inputPort, null).runEmbedded(false, 30000, exitCondition);

    Map<String, Long> data = new HashMap<>();

    for (Tuple.TimestampedTuple<KeyValPair<String, Long>> entry :
        (List<Tuple.TimestampedTuple<KeyValPair<String, Long>>>)TupleCollector.results.get("testNonStreamApplicationWordcount")) {
      data.put(entry.getValue().getKey(), entry.getValue().getValue());
    }

    //Thread.sleep(100000);
    Assert.assertNotNull(data);
    Assert.assertTrue(data.size() > 1);
    Assert.assertEquals(expected, data);
  }
}
