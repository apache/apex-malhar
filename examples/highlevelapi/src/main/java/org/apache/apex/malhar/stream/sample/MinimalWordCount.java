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
import java.util.Map;

import org.apache.apex.malhar.lib.function.Function;
import org.apache.apex.malhar.lib.util.KeyValPair;
import org.apache.apex.malhar.lib.window.TriggerOption;
import org.apache.apex.malhar.lib.window.Tuple;
import org.apache.apex.malhar.lib.window.WindowOption;
import org.apache.apex.malhar.stream.api.impl.StreamFactory;
import org.apache.hadoop.conf.Configuration;

import com.datatorrent.api.Context;
import com.datatorrent.api.DAG;
import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.api.StreamingApplication;
import com.datatorrent.api.annotation.ApplicationAnnotation;
import com.datatorrent.common.util.BaseOperator;

import static org.apache.apex.malhar.stream.api.Option.Options.name;

/**
 * Beam MinimalWordCount Example
 *
 * @since 3.5.0
 */
@ApplicationAnnotation(name = "MinimalWordCount")
public class MinimalWordCount implements StreamingApplication
{
  public static class Collector extends BaseOperator
  {
    static Map<String, Long> result;
    private static boolean done = false;

    public static boolean isDone()
    {
      return done;
    }

    @Override
    public void setup(Context.OperatorContext context)
    {
      done = false;
      result = new HashMap<>();
    }

    public final transient DefaultInputPort<KeyValPair<String, Long>> input = new DefaultInputPort<KeyValPair<String, Long>>()
    {
      @Override
      public void process(KeyValPair<String, Long> tuple)
      {
        if (tuple.getKey().equals("bye")) {
          done = true;
        }
        result.put(tuple.getKey(), tuple.getValue());
      }
    };
  }

  /**
   * Populate the dag using High-Level API.
   * @param dag
   * @param conf
   */
  @Override
  public void populateDAG(DAG dag, Configuration conf)
  {
    Collector collector = new Collector();
    // Create a stream reading from a file line by line using StreamFactory.
    StreamFactory.fromFolder("./src/test/resources/wordcount", name("textInput"))
        // Use a flatmap transformation to extract words from the incoming stream of lines.
        .flatMap(new Function.FlatMapFunction<String, String>()
        {
          @Override
          public Iterable<String> f(String input)
          {
            return Arrays.asList(input.split("[^a-zA-Z']+"));

          }
        }, name("ExtractWords"))
        // Apply windowing to the stream for counting, in this case, the window option is global window.
        .window(new WindowOption.GlobalWindow(), new TriggerOption().accumulatingFiredPanes().withEarlyFiringsAtEvery(1))
        // Count the appearances of every word.
        .countByKey(new Function.ToKeyValue<String, String, Long>()
        {
          @Override
          public Tuple<KeyValPair<String, Long>> f(String input)
          {
            return new Tuple.PlainTuple<KeyValPair<String, Long>>(new KeyValPair<String, Long>(input, 1L));
          }
        }, name("countByKey"))
        // Format the counting result to a readable format by unwrapping the tuples.
        .map(new Function.MapFunction<Tuple.WindowedTuple<KeyValPair<String, Long>>, KeyValPair<String, Long>>()
        {
          @Override
          public KeyValPair<String, Long> f(Tuple.WindowedTuple<KeyValPair<String, Long>> input)
          {
            return input.getValue();
          }
        }, name("FormatResults"))
        // Print the result.
        .print(name("console"))
        // Attach a collector to the stream to collect results.
        .endWith(collector, collector.input, name("Collector"))
        // populate the dag using the stream.
        .populateDag(dag);
  }
}
