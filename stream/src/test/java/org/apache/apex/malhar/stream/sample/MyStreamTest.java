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

import org.joda.time.Duration;
import org.junit.Assert;
import org.junit.Test;

import org.apache.apex.malhar.lib.function.Function;
import org.apache.apex.malhar.lib.util.KeyValPair;
import org.apache.apex.malhar.lib.window.TriggerOption;
import org.apache.apex.malhar.lib.window.Tuple;
import org.apache.apex.malhar.lib.window.WindowOption;
import org.apache.apex.malhar.stream.api.impl.ApexStreamImpl;
import org.apache.apex.malhar.stream.api.impl.StreamFactory;

/**
 * A test class which test your own stream implementation build on default one
 */
@SuppressWarnings("unchecked")
public class MyStreamTest
{
  static Map<String, Long> expected = new HashMap<>();
  static String testId = null;
  static Callable<Boolean> exitCondition = null;
  static {
    expected.put("newword1", 4L);
    expected.put("newword2", 8L);
    expected.put("newword3", 4L);
    expected.put("newword4", 4L);
    expected.put("newword5", 4L);
    expected.put("newword7", 4L);
    expected.put("newword9", 6L);

    exitCondition = new Callable<Boolean>()
    {
      @Override
      public Boolean call() throws Exception
      {
        if (!TupleCollector.results.containsKey(testId) || TupleCollector.results.get(testId).isEmpty()) {
          return false;
        }
        Map<String, Long> dataMap = new HashMap<>();
        List<Tuple.TimestampedTuple<KeyValPair<String, Long>>> data = (List<Tuple.TimestampedTuple<KeyValPair<String, Long>>>)TupleCollector.results.get(testId);
        for (Tuple.TimestampedTuple<KeyValPair<String, Long>> entry : data) {
          dataMap.put(entry.getValue().getKey(), entry.getValue().getValue());
        }
        return (dataMap != null) && dataMap.size() >= 1 && expected.equals(dataMap);
      }
    };
  }

  @Test
  public void testMethodChainWordcount() throws Exception
  {

    testId = "testMethodChainWordcount";

    TupleCollector<Tuple.WindowedTuple<KeyValPair<String, Long>>> collector = new TupleCollector<>();
    collector.id = testId;
    new MyStream<>((ApexStreamImpl<String>)StreamFactory.fromFolder("./src/test/resources/data"))
        .<String, MyStream<String>>flatMap(new Function.FlatMapFunction<String, String>()
        {
          @Override
          public Iterable<String> f(String input)
          {
            return Arrays.asList(input.split(" "));
          }
        }).myFilterAndMap(new Function.MapFunction<String, String>()
        {
          @Override
          public String f(String input)
          {
            return input.replace("word", "newword");
          }
        }, new Function.FilterFunction<String>()
        {
          @Override
          public boolean f(String input)
          {
            return input.startsWith("word");
          }
        }).window(new WindowOption.GlobalWindow(), new TriggerOption().accumulatingFiredPanes().withEarlyFiringsAtEvery(Duration.millis(1000)))
        .countByKey(new Function.ToKeyValue<String, String, Long>()
        {
          @Override
          public Tuple<KeyValPair<String, Long>> f(String input)
          {
            return new Tuple.PlainTuple(new KeyValPair<>(input, 1L));
          }
        }).addOperator(collector, collector.inputPort, null)
        .runEmbedded(false, 30000, exitCondition);

    Map<String, Long> dataMap = new HashMap<>();
    for (Tuple.TimestampedTuple<KeyValPair<String, Long>> entry : (List<Tuple.TimestampedTuple<KeyValPair<String, Long>>>)TupleCollector.results.get(testId)) {
      dataMap.put(entry.getValue().getKey(), entry.getValue().getValue());
    }

    Assert.assertTrue(dataMap.size() > 1);
    Assert.assertEquals(expected, dataMap);
  }

  @Test
  public void testNonMethodChainWordcount() throws Exception
  {
    testId = "testNonMethodChainWordcount";

    TupleCollector<Tuple.WindowedTuple<KeyValPair<String, Long>>> collector = new TupleCollector<>();
    collector.id = testId;
    MyStream<String> mystream = new MyStream<>((ApexStreamImpl<String>)StreamFactory
        .fromFolder("./src/test/resources/data"))
        .flatMap(new Function.FlatMapFunction<String, String>()
        {
          @Override
          public Iterable<String> f(String input)
          {
            return Arrays.asList(input.split(" "));
          }
        });
    mystream.myFilterAndMap(new Function.MapFunction<String, String>()
    {
      @Override
      public String f(String input)
      {
        return input.replace("word", "newword");
      }
    }, new Function.FilterFunction<String>()
    {
      @Override
      public boolean f(String input)
      {
        return input.startsWith("word");
      }
    }).window(new WindowOption.GlobalWindow(), new TriggerOption().accumulatingFiredPanes().withEarlyFiringsAtEvery(Duration.millis(1000)))
    .countByKey(new Function.ToKeyValue<String, String, Long>()
    {
      @Override
      public Tuple<KeyValPair<String, Long>> f(String input)
      {
        return new Tuple.PlainTuple(new KeyValPair<>(input, 1L));
      }
    }).addOperator(collector, collector.inputPort, collector.outputPort).runEmbedded(false, 30000, exitCondition);


    Map<String, Long> dataMap = new HashMap<>();
    for (Tuple.TimestampedTuple<KeyValPair<String, Long>> entry : (List<Tuple.TimestampedTuple<KeyValPair<String, Long>>>)TupleCollector.results.get(testId)) {
      dataMap.put(entry.getValue().getKey(), entry.getValue().getValue());
    }

    Assert.assertTrue(dataMap.size() > 1);
    Assert.assertEquals(expected, dataMap);
  }

}
