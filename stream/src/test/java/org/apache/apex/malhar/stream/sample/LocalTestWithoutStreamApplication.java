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

import org.apache.apex.malhar.stream.api.function.Function;
import org.apache.apex.malhar.stream.api.impl.StreamFactory;
import org.apache.apex.malhar.lib.window.Tuple;
import org.apache.apex.malhar.lib.window.WindowOption;

import com.datatorrent.lib.util.KeyValPair;

/**
 * A embedded application test without creating Streaming Application
 */
public class LocalTestWithoutStreamApplication
{
  @Test
  @SuppressWarnings("unchecked")
  public void testNonStreamApplicationWordcount() throws Exception
  {

    TupleCollector<Tuple<KeyValPair<String, Long>>> collector = new TupleCollector<>();
    collector.id = "testNonStreamApplicationWordcount";
    final Map<String, Long> expected = new HashMap<>();
    expected.put("error", 2l);
    expected.put("word1", 4l);
    expected.put("word2", 8l);
    expected.put("word3", 4l);
    expected.put("word4", 4l);
    expected.put("word5", 4l);
    expected.put("word7", 4l);
    expected.put("word9", 6l);

    Callable<Boolean> exitCondition = new Callable<Boolean>()
    {
      @Override
      public Boolean call() throws Exception
      {
        Map<Object, Integer> data = new HashMap<>();
        for (Map.Entry<Object, Integer> entry : (List<Map.Entry<Object, Integer>>)TupleCollector.results.get("testNonStreamApplicationWordcount")) {
          data.put(entry.getKey(), entry.getValue());
        }
        return data.size() >= 8 && expected.equals(data.get(data.size() - 1));
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
        .window(new WindowOption.GlobalWindow())
        .countByKey(new Function.MapFunction<String, Tuple<KeyValPair<String, Long>>>()
        {
          @Override
          public Tuple<KeyValPair<String, Long>> f(String input)
          {
            return new Tuple.PlainTuple(new KeyValPair<>(input, 1));
          }
        }).addOperator(collector, collector.inputPort, collector.outputPort).print().runEmbedded(false, 30000, exitCondition);


    List<Map<Object, Integer>> data = (List<Map<Object, Integer>>)TupleCollector.results.get("testNonStreamApplicationWordcount");

    Assert.assertNotNull(data);
    Assert.assertTrue(data.size() > 1);
    Assert.assertEquals(expected, data.get(data.size() - 1));
  }
}
