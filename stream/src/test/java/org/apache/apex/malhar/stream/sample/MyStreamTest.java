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

/**
 * A test class which test your own stream implementation build on default one
 */
@SuppressWarnings("unchecked")
public class MyStreamTest
{
  static Map<Object, Integer> expected = new HashMap<>();
  static String testId = null;
  static Callable<Boolean> exitCondition = null;
  static {
    expected.put("newword1", 4);
    expected.put("newword2", 8);
    expected.put("newword3", 4);
    expected.put("newword4", 4);
    expected.put("newword5", 4);
    expected.put("newword7", 4);
    expected.put("newword9", 6);

    exitCondition = new Callable<Boolean>()
    {
      @Override
      public Boolean call() throws Exception
      {
        List<Map<Object, Integer>> data = (List<Map<Object, Integer>>)TupleCollector.results.get(testId);
        return (data != null) && data.size() >= 1 && expected.equals(data.get(data.size() - 1));
      }
    };
  }

  @Test
  public void testMethodChainWordcount() throws Exception
  {

    testId = "testMethodChainWordcount";

    TupleCollector<Map<Object, Integer>> collector = new TupleCollector<>();
    collector.id = testId;
    new MyStream<>(StreamFactory.fromFolder("./src/test/resources/data"))
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
          public Boolean f(String input)
          {
            return input.startsWith("word");
          }
        }).countByKey()
        .addOperator(collector, collector.inputPort, collector.outputPort).print().runEmbedded(false, 30000, exitCondition);


    List<Map<Object, Integer>> data = (List<Map<Object, Integer>>)TupleCollector.results.get(testId);
    Assert.assertTrue(data.size() > 1);
    Assert.assertEquals(expected, data.get(data.size() - 1));
  }

  @Test
  public void testNonMethodChainWordcount() throws Exception
  {
    testId = "testNonMethodChainWordcount";

    TupleCollector<Map<Object, Integer>> collector = new TupleCollector<>();
    collector.id = testId;
    MyStream<String> mystream = new MyStream<>(StreamFactory
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
      public Boolean f(String input)
      {
        return input.startsWith("word");
      }
    }).countByKey().addOperator(collector, collector.inputPort, collector.outputPort).print().runEmbedded(false, 30000, exitCondition);


    List<Map<Object, Integer>> data = (List<Map<Object, Integer>>)TupleCollector.results.get(testId);
    Assert.assertTrue(data.size() > 1);
    Assert.assertEquals(expected, data.get(data.size() - 1));
  }

}
