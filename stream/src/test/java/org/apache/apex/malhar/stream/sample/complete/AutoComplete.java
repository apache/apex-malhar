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
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.joda.time.Duration;

import org.apache.apex.malhar.stream.api.ApexStream;
import org.apache.apex.malhar.stream.api.CompositeStreamTransform;
import org.apache.apex.malhar.stream.api.WindowedStream;
import org.apache.apex.malhar.stream.api.function.Function;
import org.apache.apex.malhar.stream.api.impl.StreamFactory;
import org.apache.apex.malhar.lib.window.TriggerOption;
import org.apache.apex.malhar.lib.window.Tuple;
import org.apache.apex.malhar.lib.window.WindowOption;
import org.apache.commons.io.IOUtils;

import com.google.common.base.Throwables;
import com.google.common.collect.Sets;

import com.datatorrent.api.Context;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.api.InputOperator;
import com.datatorrent.common.util.BaseOperator;

import com.datatorrent.lib.util.KeyValPair;

/**
 * An example that computes the most popular hash tags
 * for every prefix, which can be used for auto-completion.
 *
 * <p>This will update the datastore every 10 seconds based on the last
 * 30 minutes of data received.
 */
public class AutoComplete
{


  public static class TweetsInput extends BaseOperator implements InputOperator
  {
    public final transient DefaultOutputPort<String> output = new DefaultOutputPort<>();

    private transient BufferedReader reader;

    @Override
    public void setup(Context.OperatorContext context)
    {
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
      try {
        String line = reader.readLine();
        if (line == null) {
          reader.close();
          initReader();
        } else {
          this.output.emit(reader.readLine());
        }
        Thread.sleep(500);
      } catch (IOException ex) {
        throw new RuntimeException(ex);
      } catch (InterruptedException e) {
        e.printStackTrace();
      }
    }

  }

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
   * Class used to store tag-count pairs.
   */
  public static class CompletionCandidate implements Comparable<CompletionCandidate> {

    private long count;
    private String value;

    public CompletionCandidate(String value, long count) {
      this.value = value;
      this.count = count;
    }

    public long getCount() {
      return count;
    }

    public String getValue() {
      return value;
    }

    // Empty constructor required for Avro decoding.
    public CompletionCandidate() {}

    @Override
    public int compareTo(CompletionCandidate o) {
      if (this.count < o.count) {
        return -1;
      } else if (this.count == o.count) {
        return this.value.compareTo(o.value);
      } else {
        return 1;
      }
    }

    @Override
    public boolean equals(Object other) {
      if (other instanceof CompletionCandidate) {
        CompletionCandidate that = (CompletionCandidate) other;
        return this.count == that.count && this.value.equals(that.value);
      } else {
        return false;
      }
    }

    @Override
    public int hashCode() {
      return Long.valueOf(count).hashCode() ^ value.hashCode();
    }

    @Override
    public String toString() {
      return "CompletionCandidate[" + value + ", " + count + "]";
    }
  }



  /**
   * Lower latency, but more expensive.
   */
  private static class ComputeTopFlat
      extends CompositeStreamTransform<CompletionCandidate, KeyValPair<String, List<CompletionCandidate>>>
  {
    private final int candidatesPerPrefix;
    private final int minPrefix;

    public ComputeTopFlat(int candidatesPerPrefix, int minPrefix)
    {
      this.candidatesPerPrefix = candidatesPerPrefix;
      this.minPrefix = minPrefix;
    }

    @Override
    public ApexStream<KeyValPair<String, List<CompletionCandidate>>> compose(
        ApexStream<CompletionCandidate> input)
    {
      return input.flatMap(new AllPrefixes(minPrefix)).window(new WindowOption.GlobalWindow())
          .topByKey(1, new Function.MapFunction<KeyValPair<String, CompletionCandidate>, KeyValPair<String,
              CompletionCandidate>>()
          {
            @Override
            public KeyValPair<String, CompletionCandidate> f(KeyValPair<String, CompletionCandidate> tuple)
            {
              return tuple;
            }
          });
    }
  }


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
        result.add(new KeyValPair<>(input.getValue(), input));
      }
      return result;
    }
  }

  /**
   * A Composite stream transform that takes as input a list of tokens and returns
   * the most common tokens per prefix.
   */
  public static class ComputeTopCompletions
      extends CompositeStreamTransform<String, KeyValPair<String, List<CompletionCandidate>>>
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
    public ApexStream<KeyValPair<String, List<CompletionCandidate>>> compose(ApexStream<String> inputStream)
    {
      if (!(inputStream instanceof WindowedStream)) {
        return null;
      }

      ApexStream<CompletionCandidate> candidates = ((WindowedStream<String>)inputStream)
          .countByKey(new Function.MapFunction<String, Tuple<KeyValPair<String, Long>>>()
          {
            @Override
            public Tuple<KeyValPair<String, Long>> f(String input)
            {
              return new Tuple.PlainTuple<>(new KeyValPair<>(input, 1l));
            }
          }).map(new Function.MapFunction<Tuple<KeyValPair<String, Long>>, CompletionCandidate>()

          {
            @Override
            public CompletionCandidate f(Tuple<KeyValPair<String, Long>> input)
            {
              return new CompletionCandidate(input.getValue().getKey(), input.getValue().getValue());
            }
          });

      return candidates.addCompositeStreams(new ComputeTopFlat(10, 1));

    }
  }






  public static void main(String[] args)
  {
    boolean stream = Sets.newHashSet(args).contains("--streaming");
    TweetsInput input = new TweetsInput();

    WindowOption windowOption = stream
        ? new WindowOption.TimeWindows(Duration.standardMinutes(30)).slideBy(Duration.standardSeconds(5))
        : new WindowOption.GlobalWindow();

    ApexStream<String> tags = StreamFactory.fromInput(input, input.output)
        .flatMap(new ExtractHashtags());
    tags.print();
        tags.window(windowOption, new TriggerOption().accumulatingFiredPanes().withEarlyFiringsAtEvery(Duration.standardSeconds(10)))
        .addCompositeStreams(ComputeTopCompletions.top(10, true)).print()
        .runEmbedded(false, 100000, new Callable<Boolean>()
        {
          @Override
          public Boolean call() throws Exception
          {
            return false;
          }
        });

  }
}
