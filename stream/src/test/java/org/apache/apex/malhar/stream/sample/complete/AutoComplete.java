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

import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.apex.malhar.stream.api.ApexStream;
import org.apache.apex.malhar.stream.api.CompositeStreamTransform;
import org.apache.apex.malhar.stream.api.WindowedStream;
import org.apache.apex.malhar.stream.api.function.Function;
import org.apache.apex.malhar.stream.api.impl.StreamFactory;

import com.google.common.collect.Sets;

import com.datatorrent.contrib.twitter.TwitterSampleInput;
import com.datatorrent.lib.util.KeyValPair;
import com.datatorrent.lib.window.WindowOption;

import static com.datatorrent.lib.window.Quantification.TimeUnit.*;
import static com.datatorrent.lib.window.WindowOption.WindowOptionBuilder.*;

/**
 * An example that computes the most popular hash tags
 * for every prefix, which can be used for auto-completion.
 *
 * <p>This will update the datastore every 10 seconds based on the last
 * 30 minutes of data received.
 */
public class AutoComplete
{

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
  static class CompletionCandidate implements Comparable<CompletionCandidate> {
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
      extends CompositeStreamTransform<CompletionCandidate, Map.Entry<String, List<CompletionCandidate>>>
  {
    private final int candidatesPerPrefix;
    private final int minPrefix;

    public ComputeTopFlat(int candidatesPerPrefix, int minPrefix)
    {
      this.candidatesPerPrefix = candidatesPerPrefix;
      this.minPrefix = minPrefix;
    }

    @Override
    public ApexStream<Map.Entry<String, List<CompletionCandidate>>> compose(
        ApexStream<CompletionCandidate> input)
    {
      return input.map(new AllPrefixes(minPrefix)).window(all()).topByKey(1);
    }
  }


  private static class AllPrefixes implements Function.FlatMapFunction<CompletionCandidate, Map.Entry<String, CompletionCandidate>>
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
    public Iterable<Map.Entry<String, CompletionCandidate>> f(CompletionCandidate input)
    {
      List<Map.Entry<String, CompletionCandidate>> result = new LinkedList<>();
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
      extends CompositeStreamTransform<String, Map.Entry<String, List<CompletionCandidate>>>
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
    public ApexStream<Map.Entry<String, List<CompletionCandidate>>> compose(ApexStream<String> inputStream)
    {
      if (!(inputStream instanceof WindowedStream)) {
        return null;
      }

      ApexStream<CompletionCandidate> candidates = ((WindowedStream)inputStream).countByKey()
          .map(new Function.MapFunction<Map.Entry<Object, Integer>, CompletionCandidate>()
          {
            @Override
            public CompletionCandidate f(Map.Entry<Object, Integer> input)
            {
              return new CompletionCandidate((String)input.getKey(), input.getValue());
            }
          });

      return candidates.addCompositeStreams(new ComputeTopFlat(10, 1));

    }
  }






  public static void main(String[] args)
  {
    boolean stream = Sets.newHashSet(args).contains("--streaming");
    TwitterSampleInput input = new TwitterSampleInput();

    WindowOption windowOption = stream
        ? WindowOption.WindowOptionBuilder.intoEvery(30, MINUTE).slideBy(5, MILLISECOND)
        : all();

    StreamFactory.fromInput(new TwitterSampleInput(), input.text)
        .flatMap(new ExtractHashtags())
        .window(windowOption)
        .addCompositeStreams(ComputeTopCompletions.top(10, true));

  }
}
