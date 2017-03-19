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
package org.apache.apex.malhar.stream.sample.cookbook;

import java.util.Arrays;
import java.util.List;

import org.joda.time.Duration;

import org.apache.apex.malhar.lib.function.Function;
import org.apache.apex.malhar.lib.window.TriggerOption;
import org.apache.apex.malhar.lib.window.Tuple;
import org.apache.apex.malhar.lib.window.WindowOption;
import org.apache.apex.malhar.lib.window.accumulation.RemoveDuplicates;
import org.apache.apex.malhar.stream.api.ApexStream;
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
 * Beam DeDupExample.
 *
 * @since 3.5.0
 */
@ApplicationAnnotation(name = "DeDupExample")
public class DeDupExample implements StreamingApplication
{

  public static class Collector extends BaseOperator
  {
    private static Tuple.WindowedTuple<List<String>> result;
    private static boolean done = false;

    public static Tuple.WindowedTuple<List<String>> getResult()
    {
      return result;
    }

    public static boolean isDone()
    {
      return done;
    }

    @Override
    public void setup(Context.OperatorContext context)
    {
      super.setup(context);
      result = new Tuple.WindowedTuple<>();
      done = false;
    }

    public transient DefaultInputPort<Tuple.WindowedTuple<List<String>>> input = new DefaultInputPort<Tuple.WindowedTuple<List<String>>>()
    {
      @Override
      public void process(Tuple.WindowedTuple<List<String>> tuple)
      {
        result = tuple;
        if (result.getValue().contains("bye")) {
          done = true;
        }
      }
    };
  }

  @Override
  public void populateDAG(DAG dag, Configuration conf)
  {
    Collector collector = new Collector();

    // Create a stream that reads from files in a local folder and output lines one by one to downstream.
    ApexStream<String> stream = StreamFactory.fromFolder("./src/test/resources/wordcount", name("textInput"))

        // Extract all the words from the input line of text.
        .flatMap(new Function.FlatMapFunction<String, String>()
        {
          @Override
          public Iterable<String> f(String input)
          {
            return Arrays.asList(input.split("[\\p{Punct}\\s]+"));
          }
        }, name("ExtractWords"))

        // Change the words to lower case, also shutdown the app when the word "bye" is detected.
        .map(new Function.MapFunction<String, String>()
        {
          @Override
          public String f(String input)
          {
            return input.toLowerCase();
          }
        }, name("ToLowerCase"));

    // Apply window and trigger option.
    stream.window(new WindowOption.GlobalWindow(),
        new TriggerOption().accumulatingFiredPanes().withEarlyFiringsAtEvery(Duration.standardSeconds(1)))

        // Remove the duplicate words and print out the result.
        .accumulate(new RemoveDuplicates<String>(), name("RemoveDuplicates"))
        .print(name("console"))
        .endWith(collector, collector.input)
        .populateDag(dag);
  }
}
