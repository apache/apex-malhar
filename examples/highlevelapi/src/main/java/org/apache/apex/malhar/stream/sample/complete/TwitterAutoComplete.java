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

import java.nio.charset.Charset;
import java.nio.charset.CharsetEncoder;

import java.util.LinkedList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.joda.time.Duration;

import org.apache.apex.malhar.contrib.twitter.TwitterSampleInput;
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

import org.apache.hadoop.conf.Configuration;

import com.datatorrent.api.DAG;
import com.datatorrent.api.StreamingApplication;
import com.datatorrent.api.annotation.ApplicationAnnotation;

import static org.apache.apex.malhar.stream.api.Option.Options.name;

/**
 * Auto Complete Hashtag Example with real time twitter input. In order to run this application, you need to create an app
 * at https://apps.twitter.com, then generate your consumer and access keys and tokens, and enter those information
 * accordingly in /resources/META-INF/properties.xml.
 *
 * The authentication requires following 4 information.
 * Your application consumer key,
 * Your application consumer secret,
 * Your twitter access token, and
 * Your twitter access token secret.
 *
 * @since 3.5.0
 */
@ApplicationAnnotation(name = "TwitterAutoComplete")
public class TwitterAutoComplete implements StreamingApplication
{
  /**
   * Check whether every character in a string is ASCII encoding.
   */
  public static class StringUtils
  {
    static CharsetEncoder encoder = Charset.forName("US-ASCII").newEncoder();

    public static boolean isAscii(String v)
    {
      return encoder.canEncode(v);
    }
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
      TopNByKey topNByKey = new TopNByKey();
      topNByKey.setN(candidatesPerPrefix);
      return input
        .<KeyValPair<String, CompletionCandidate>, WindowedStream<KeyValPair<String, CompletionCandidate>>>flatMap(new AllPrefixes(minPrefix, 3), name("Extract Prefixes"))
        .accumulateByKey(topNByKey, new Function.ToKeyValue<KeyValPair<String, CompletionCandidate>, String, CompletionCandidate>()
        {
          @Override
          public Tuple<KeyValPair<String, CompletionCandidate>> f(KeyValPair<String, CompletionCandidate> tuple)
          {
            // TODO: Should be removed after Auto-wrapping is supported.
            return new Tuple.WindowedTuple<>(Window.GlobalWindow.INSTANCE, tuple);
          }
        }, name("TopNByKey"));
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
          }, name("Hashtag Count"))
          .map(new Function.MapFunction<Tuple.WindowedTuple<KeyValPair<String,Long>>, CompletionCandidate>()
          {
            @Override
            public CompletionCandidate f(Tuple.WindowedTuple<KeyValPair<String, Long>> input)
            {
              return new CompletionCandidate(input.getValue().getKey(), input.getValue().getValue());
            }
          }, name("KeyValPair to CompletionCandidate"));

      return candidates.addCompositeStreams(new ComputeTopFlat(candidatesPerPrefix, 1));

    }
  }

  /**
   * FilterFunction to filter out tweets with non-acsii characters.
   */
  static class ASCIIFilter implements Function.FilterFunction<String>
  {
    @Override
    public boolean f(String input)
    {
      return StringUtils.isAscii(input);
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
    TwitterSampleInput input = new TwitterSampleInput();

    WindowOption windowOption = new WindowOption.GlobalWindow();

    ApexStream<String> tags = StreamFactory.fromInput(input, input.text, name("tweetSampler"))
        .filter(new ASCIIFilter(), name("ACSII Filter"))
        .flatMap(new ExtractHashtags(), name("Extract Hashtags"));

    ApexStream<Tuple.WindowedTuple<KeyValPair<String, List<CompletionCandidate>>>> s =
        tags.window(windowOption, new TriggerOption().accumulatingFiredPanes().withEarlyFiringsAtEvery(Duration.standardSeconds(10)))
        .addCompositeStreams(ComputeTopCompletions.top(10, true)).print();

    s.populateDag(dag);
  }
}
