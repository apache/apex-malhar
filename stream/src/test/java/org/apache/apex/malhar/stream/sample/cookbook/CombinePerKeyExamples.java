/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.apex.malhar.stream.sample.cookbook;

import org.apache.apex.malhar.stream.api.ApexStream;
import org.apache.apex.malhar.stream.api.CompositeStreamTransform;
import org.apache.apex.malhar.stream.api.function.Function;
import org.apache.apex.malhar.stream.api.impl.StreamFactory;
import org.apache.apex.malhar.lib.window.WindowOption;


import com.datatorrent.api.Context;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.api.InputOperator;
import com.datatorrent.lib.util.KeyValPair;

/**
 * An example that reads the public 'Shakespeare' data, and for each word in
 * the dataset that is over a given length, generates a string containing the
 * list of play names in which that word appears
 *
 * <p>Concepts: the combine transform, which lets you combine the values in a
 * key-grouped Collection
 *
 */
public class CombinePerKeyExamples {
  // Use the shakespeare public BigQuery sample
  private static final String SHAKESPEARE_TABLE =
      "publicdata:samples.shakespeare";
  // We'll track words >= this word length across all plays in the table.
  private static final int MIN_WORD_LENGTH = 9;

  /**
   * Examines each row in the input table. If the word is greater than or equal to MIN_WORD_LENGTH,
   * outputs word, play_name.
   */
  static class ExtractLargeWordsFn implements Function.MapFunction<SampleBean, KeyValPair<String, String>>
  {

    @Override
    public KeyValPair<String, String> f(SampleBean input)
    {
      String playName = input.getCorpus();
      String word = input.getWord();
      if (word.length() >= MIN_WORD_LENGTH) {
        return new KeyValPair<>(word, playName);
      } else {
        return null;
      }
    }
  }


  /**
   * Prepares the output data which is in same bean
   */
  static class FormatShakespeareOutputFn implements Function.MapFunction<KeyValPair<String, String>, SampleBean>
  {
    @Override
    public SampleBean f(KeyValPair<String, String> input)
    {
      return new SampleBean(input.getKey(), input.getValue(), null);
    }
  }

  /**
   * Reads the public 'Shakespeare' data, and for each word in the dataset
   * over a given length, generates a string containing the list of play names
   * in which that word appears.
   */
  static class PlaysForWord
      extends CompositeStreamTransform<SampleBean, SampleBean>
  {

    @Override
    public ApexStream<SampleBean> compose(ApexStream<SampleBean> inputStream)
    {
      return inputStream.map(new ExtractLargeWordsFn())
          .window(new WindowOption.GlobalWindow())
          .foldByKey(new Function.FoldFunction<KeyValPair<String, String>, KeyValPair<String, String>>()
          {
            @Override
            public KeyValPair<String, String> fold(KeyValPair<String, String> input, KeyValPair<String, String> output)
            {
              output.setValue(output.getValue() + input.getValue());
              return output;
            }
          })
          .map(new FormatShakespeareOutputFn());
    }
  }


  public static class SampleBean
  {

    public SampleBean()
    {

    }

    public SampleBean(String word, String all_plays, String corpus)
    {
      this.word = word;
      this.all_plays = all_plays;
      this.corpus = corpus;
    }

    private String word;

    private String all_plays;

    private String corpus;

    public void setWord(String word)
    {
      this.word = word;
    }

    public String getWord()
    {
      return word;
    }

    public void setCorpus(String corpus)
    {
      this.corpus = corpus;
    }

    public String getCorpus()
    {
      return corpus;
    }

    public void setAll_plays(String all_plays)
    {
      this.all_plays = all_plays;
    }

    public String getAll_plays()
    {
      return all_plays;
    }
  }

  public static class SampleInput implements InputOperator
  {

    public final transient DefaultOutputPort<SampleBean> beanOutput= new DefaultOutputPort();

    @Override
    public void emitTuples()
    {

    }

    @Override
    public void beginWindow(long l)
    {

    }

    @Override
    public void endWindow()
    {

    }

    @Override
    public void setup(Context.OperatorContext context)
    {

    }

    @Override
    public void teardown()
    {

    }
  }


  public static void main(String[] args)
      throws Exception {
    SampleInput input = new SampleInput();
    StreamFactory.fromInput(input, input.beanOutput)
        .addCompositeStreams(new PlaysForWord());
  }
}
