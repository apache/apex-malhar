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
package org.apache.apex.malhar.stream.sample.complete;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.apex.malhar.lib.function.Function;
import org.apache.apex.malhar.lib.util.KeyValPair;
import org.apache.apex.malhar.lib.window.TriggerOption;
import org.apache.apex.malhar.lib.window.Tuple;
import org.apache.apex.malhar.lib.window.Window;
import org.apache.apex.malhar.lib.window.WindowOption;
import org.apache.apex.malhar.stream.api.ApexStream;
import org.apache.apex.malhar.stream.api.CompositeStreamTransform;
import org.apache.apex.malhar.stream.api.WindowedStream;
import org.apache.apex.malhar.stream.api.impl.StreamFactory;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;

import com.google.common.base.Throwables;

import com.datatorrent.api.Context.OperatorContext;
import com.datatorrent.api.DAG;
import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.api.InputOperator;
import com.datatorrent.api.StreamingApplication;
import com.datatorrent.api.annotation.ApplicationAnnotation;
import com.datatorrent.common.util.BaseOperator;

import static org.apache.apex.malhar.stream.api.Option.Options.name;

/**
 * An example that computes the most popular hash tags
 * for every prefix, which can be used for auto-completion.
 * This application is identical to TwitterAutoComplete, except it's
 * reading from a file. This application is mainly for local testing
 * purpose.
 *
 * <p>This will update the datastore every 10 seconds based on the last
 * 30 minutes of data received.
 *
 * @since 3.5.0
 */
@ApplicationAnnotation(name = "AutoComplete")
public class AutoComplete implements StreamingApplication
{

  /**
   * A dummy Twitter input operator. It reads from a text file containing some tweets and output a line every
   * half of a second.
   */
  public static class TweetsInput extends BaseOperator implements InputOperator
  {
    public final transient DefaultOutputPort<String> output = new DefaultOutputPort<>();
    private boolean done;

    private transient BufferedReader reader;

    @Override
    public void setup(OperatorContext context)
    {
      done = false;
      initReader();
    }

    private void initReader()
    {
      try {
        InputStream resourceStream = this.getClass().getResourceAsStream("/sampletweets.txt");
        reader = new BufferedReader(new InputStreamReader(resourceStream));
      } catch (Exception ex) {
        throw Throwables.propagate(ex);
      }
    }

    @Override
    public void teardown()
    {
      IOUtils.closeQuietly(reader);
    }

    @Override
    public void emitTuples()
    {
      if (!done) {
        try {
          String line = reader.readLine();
          if (line == null) {
            done = true;
            reader.close();
          } else {
            this.output.emit(line);
          }
          Thread.sleep(50);
        } catch (IOException ex) {
          throw new RuntimeException(ex);
        } catch (InterruptedException e) {
          // Ignore it.
        }
      }
    }
  }

  public static class Collector extends BaseOperator
  {
    private static Map<String, List<CompletionCandidate>> result = new HashMap<>();
    private static boolean done = false;

    public static boolean isDone()
    {
      return done;
    }

    @Override
    public void setup(OperatorContext context)
    {
      super.setup(context);
      done = false;
    }

    public static Map<String, List<CompletionCandidate>> getResult()
    {
      return result;
    }

    public final transient DefaultInputPort<Tuple.WindowedTuple<KeyValPair<String, List<CompletionCandidate>>>> input = new DefaultInputPort<Tuple.WindowedTuple<KeyValPair<String, List<CompletionCandidate>>>>()
    {
      @Override
      public void process(Tuple.WindowedTuple<KeyValPair<String, List<CompletionCandidate>>> tuple)
      {
        if (tuple.getValue().getKey().equals("yarn")) {
          done = true;
        }
        result.put(tuple.getValue().getKey(), tuple.getValue().getValue());
      }
    };
  }

  /**
   * FlapMap Function to extract all hashtags from a text form tweet.
   */
  private static class ExtractHashtags implements Function.FlatMapFunction<String, String>
  {

    @Override
    public Iterable<String> f(String input)
    {
      List<String> result = new LinkedList<>();
      Matcher m = Pattern.compile("#\\S+").matcher(input);
      while (m.find()) {
        result.add(m.group().substring(1));
      }
      return result;
    }
  }

  /**
   * Lower latency, but more expensive.
   */
  private static class ComputeTopFlat
      extends CompositeStreamTransform<WindowedStream<CompletionCandidate>, WindowedStream<Tuple.WindowedTuple<KeyValPair<String, List<CompletionCandidate>>>>>
  {
    private final int candidatesPerPrefix;
    private final int minPrefix;

    public ComputeTopFlat(int candidatesPerPrefix, int minPrefix)
    {
      this.candidatesPerPrefix = candidatesPerPrefix;
      this.minPrefix = minPrefix;
    }

    @Override
    public WindowedStream<Tuple.WindowedTuple<KeyValPair<String, List<CompletionCandidate>>>> compose(
        WindowedStream<CompletionCandidate> input)
    {
      return input
        .<KeyValPair<String, CompletionCandidate>, WindowedStream<KeyValPair<String, CompletionCandidate>>>flatMap(new AllPrefixes(minPrefix))
        .accumulateByKey(new TopNByKey(), new Function.ToKeyValue<KeyValPair<String, CompletionCandidate>, String,
          CompletionCandidate>()
        {
          @Override
          public Tuple<KeyValPair<String, CompletionCandidate>> f(KeyValPair<String, CompletionCandidate> tuple)
          {
            // TODO: Should be removed after Auto-wrapping is supported.
            return new Tuple.WindowedTuple<>(Window.GlobalWindow.INSTANCE, tuple);
          }
        });
    }
  }

  /**
   * FlapMap Function to extract all prefixes of the hashtag in the input CompletionCandidate, and output
   * KeyValPairs of the prefix and the CompletionCandidate
   */
  private static class AllPrefixes implements Function.FlatMapFunction<CompletionCandidate, KeyValPair<String, CompletionCandidate>>
  {
    private final int minPrefix;
    private final int maxPrefix;

    public AllPrefixes()
    {
      this(0, Integer.MAX_VALUE);
    }

    public AllPrefixes(int minPrefix)
    {
      this(minPrefix, Integer.MAX_VALUE);
    }

    public AllPrefixes(int minPrefix, int maxPrefix)
    {
      this.minPrefix = minPrefix;
      this.maxPrefix = maxPrefix;
    }

    @Override
    public Iterable<KeyValPair<String, CompletionCandidate>> f(CompletionCandidate input)
    {
      List<KeyValPair<String, CompletionCandidate>> result = new LinkedList<>();
      String word = input.getValue();
      for (int i = minPrefix; i <= Math.min(word.length(), maxPrefix); i++) {

        result.add(new KeyValPair<>(input.getValue().substring(0, i).toLowerCase(), input));
      }
      return result;
    }
  }

  /**
   * A Composite stream transform that takes as input a list of tokens and returns
   * the most common tokens per prefix.
   */
  public static class ComputeTopCompletions
      extends CompositeStreamTransform<WindowedStream<String>, WindowedStream<Tuple.WindowedTuple<KeyValPair<String, List<CompletionCandidate>>>>>
  {
    private final int candidatesPerPrefix;
    private final boolean recursive;

    protected ComputeTopCompletions(int candidatesPerPrefix, boolean recursive)
    {
      this.candidatesPerPrefix = candidatesPerPrefix;
      this.recursive = recursive;
    }

    public static ComputeTopCompletions top(int candidatesPerPrefix, boolean recursive)
    {
      return new ComputeTopCompletions(candidatesPerPrefix, recursive);
    }

    @Override
    @SuppressWarnings("unchecked")
    public WindowedStream<Tuple.WindowedTuple<KeyValPair<String, List<CompletionCandidate>>>> compose(WindowedStream<String> inputStream)
    {
      ApexStream<CompletionCandidate> candidates = inputStream
          .countByKey(new Function.ToKeyValue<String, String, Long>()
          {
            @Override
            public Tuple<KeyValPair<String, Long>> f(String input)
            {
              return new Tuple.PlainTuple<>(new KeyValPair<>(input, 1L));
            }
          }, name("countByKey"))
          .map(new Function.MapFunction<Tuple.WindowedTuple<KeyValPair<String,Long>>, CompletionCandidate>()
          {
            @Override
            public CompletionCandidate f(Tuple.WindowedTuple<KeyValPair<String, Long>> input)
            {
              return new CompletionCandidate(input.getValue().getKey(), input.getValue().getValue());
            }
          }, name("ToCompletionCandidate"));

      return candidates.addCompositeStreams(new ComputeTopFlat(10, 1));

    }
  }

  /**
   * Populate the dag with High-Level API.
   * @param dag
   * @param conf
   */
  @Override
  public void populateDAG(DAG dag, Configuration conf)
  {
    TweetsInput input = new TweetsInput();
    Collector collector = new Collector();

    WindowOption windowOption = new WindowOption.GlobalWindow();

    ApexStream<String> tags = StreamFactory.fromInput(input, input.output, name("tweetSampler"))
        .flatMap(new ExtractHashtags());

    tags.window(windowOption, new TriggerOption().accumulatingFiredPanes().withEarlyFiringsAtEvery(1))
        .addCompositeStreams(ComputeTopCompletions.top(10, true)).print(name("console"))
        .endWith(collector, collector.input, name("collector"))
        .populateDag(dag);
  }
}
