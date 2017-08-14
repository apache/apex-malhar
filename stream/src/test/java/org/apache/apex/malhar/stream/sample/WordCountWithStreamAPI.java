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

import org.joda.time.Duration;

import org.apache.apex.malhar.lib.function.Function;
import org.apache.apex.malhar.lib.util.KeyValPair;
import org.apache.apex.malhar.lib.window.TriggerOption;
import org.apache.apex.malhar.lib.window.Tuple;
import org.apache.apex.malhar.lib.window.WindowOption;
import org.apache.apex.malhar.stream.api.ApexStream;
import org.apache.apex.malhar.stream.api.impl.StreamFactory;
import org.apache.hadoop.conf.Configuration;

import com.datatorrent.api.DAG;
import com.datatorrent.api.StreamingApplication;
import com.datatorrent.api.annotation.ApplicationAnnotation;

/**
 * Word count with streaming API
 */
@ApplicationAnnotation(name = "WCDemo")
public class WordCountWithStreamAPI implements StreamingApplication
{

  @Override
  public void populateDAG(DAG dag, Configuration configuration)
  {
    WCInput wcInput = new WCInput();
    ApexStream<String> stream = StreamFactory
        .fromInput(wcInput, wcInput.output)
        .flatMap(new Function.FlatMapFunction<String, String>()
        {
          @Override
          public Iterable<String> f(String input)
          {
            return Arrays.asList(input.split("[\\p{Punct}\\s]+"));
          }
        });
    stream.print();
    stream.window(new WindowOption.GlobalWindow(), new TriggerOption().withEarlyFiringsAtEvery(Duration
        .millis(1000)).accumulatingFiredPanes())
        .countByKey(new Function.ToKeyValue<String, String, Long>()
        {
          @Override
          public Tuple<KeyValPair<String, Long>> f(String input)
          {
            return new Tuple.PlainTuple(new KeyValPair<>(input, 1L));
          }
        }).print();
    stream.populateDag(dag);
  }
}
