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

import java.util.ArrayList;
import java.util.List;

import org.apache.apex.malhar.lib.function.Function;
import org.apache.apex.malhar.lib.util.KeyValPair;
import org.apache.apex.malhar.lib.window.TriggerOption;
import org.apache.apex.malhar.lib.window.Tuple;
import org.apache.apex.malhar.lib.window.WindowOption;
import org.apache.apex.malhar.lib.window.accumulation.ReduceFn;
import org.apache.apex.malhar.stream.api.ApexStream;
import org.apache.apex.malhar.stream.api.CompositeStreamTransform;
import org.apache.apex.malhar.stream.api.WindowedStream;
import org.apache.apex.malhar.stream.api.impl.StreamFactory;

import org.apache.hadoop.conf.Configuration;

import com.datatorrent.api.Context;
import com.datatorrent.api.DAG;
import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.api.InputOperator;
import com.datatorrent.api.StreamingApplication;
import com.datatorrent.api.annotation.ApplicationAnnotation;
import com.datatorrent.common.util.BaseOperator;

import static org.apache.apex.malhar.stream.api.Option.Options.name;

/**
 * An example that reads the public 'Shakespeare' data, and for each word in
 * the dataset that is over a given length, generates a string containing the
 * list of play names in which that word appears
 *
 * <p>Concepts: the combine transform, which lets you combine the values in a
 * key-grouped Collection
 *
 *
 * @since 3.5.0
 */
@ApplicationAnnotation(name = "CombinePerKeyExamples")
public class CombinePerKeyExamples implements StreamingApplication
{
  // Use the shakespeare public BigQuery sample
  private static final String SHAKESPEARE_TABLE = "publicdata:samples.shakespeare";
  // We'll track words >= this word length across all plays in the table.
  private static final int MIN_WORD_LENGTH = 0;

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
  static class FormatShakespeareOutputFn implements Function.MapFunction<Tuple.WindowedTuple<KeyValPair<String, String>>, SampleBean>
  {
    @Override
    public SampleBean f(Tuple.WindowedTuple<KeyValPair<String, String>> input)
    {
      return new SampleBean(input.getValue().getKey(), input.getValue().getValue());
    }
  }

  /**
   * A reduce function to concat two strings together.
   */
  public static class Concat extends ReduceFn<String>
  {
    @Override
    public String reduce(String input1, String input2)
    {
      return input1 + ", " + input2;
    }
  }

  /**
   * Reads the public 'Shakespeare' data, and for each word in the dataset
   * over a given length, generates a string containing the list of play names
   * in which that word appears.
   */
  private static class PlaysForWord extends CompositeStreamTransform<ApexStream<SampleBean>, WindowedStream<SampleBean>>
  {

    @Override
    public WindowedStream<SampleBean> compose(ApexStream<SampleBean> inputStream)
    {
      return inputStream
          // Extract words from the input SampleBeam stream.
          .map(new ExtractLargeWordsFn(), name("ExtractLargeWordsFn"))

          // Apply window and trigger option to the streams.
          .window(new WindowOption.GlobalWindow(), new TriggerOption().accumulatingFiredPanes().withEarlyFiringsAtEvery(1))

          // Apply reduceByKey transformation to concat the names of all the plays that a word has appeared in together.
          .reduceByKey(new Concat(), new Function.ToKeyValue<KeyValPair<String,String>, String, String>()
          {
            @Override
            public Tuple<KeyValPair<String, String>> f(KeyValPair<String, String> input)
            {
              return new Tuple.PlainTuple<KeyValPair<String, String>>(input);
            }
          }, name("Concat"))

          // Format the output back to a SampleBeam object.
          .map(new FormatShakespeareOutputFn(), name("FormatShakespeareOutputFn"));
    }
  }


  /**
   * A Java Beam class that contains information about a word appears in a corpus written by Shakespeare.
   */
  public static class SampleBean
  {

    public SampleBean()
    {

    }

    public SampleBean(String word, String corpus)
    {
      this.word = word;
      this.corpus = corpus;
    }

    @Override
    public String toString()
    {
      return this.word + " : "  + this.corpus;
    }

    private String word;

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
  }

  /**
   * A dummy info generator to generate {@link SampleBean} objects to mimic reading from real 'Shakespeare'
   * data.
   */
  public static class SampleInput extends BaseOperator implements InputOperator
  {

    public final transient DefaultOutputPort<SampleBean> beanOutput = new DefaultOutputPort();
    private String[] words = new String[]{"A", "B", "C", "D", "E", "F", "G"};
    private String[] corpuses = new String[]{"1", "2", "3", "4", "5", "6", "7", "8"};
    private static int i;

    @Override
    public void setup(Context.OperatorContext context)
    {
      super.setup(context);
      i = 0;
    }

    @Override
    public void emitTuples()
    {
      while (i < 1) {
        for (String word : words) {
          for (String corpus : corpuses) {
            try {
              Thread.sleep(50);
              beanOutput.emit(new SampleBean(word, corpus));
            } catch (Exception e) {
              // Ignore it
            }
          }
        }
        i++;
      }

    }
  }

  public static class Collector extends BaseOperator
  {
    private static List<SampleBean> result;
    private static boolean done = false;

    public static List<SampleBean> getResult()
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
      result = new ArrayList<>();
      done = false;
    }

    public final transient DefaultInputPort<SampleBean> input = new DefaultInputPort<SampleBean>()
    {
      @Override
      public void process(SampleBean tuple)
      {
        if (tuple.getWord().equals("F")) {
          done = true;
        }
        result.add(tuple);
      }
    };
  }

  /**
   * Populate dag using High-Level API.
   * @param dag
   * @param conf
   */
  @Override
  public void populateDAG(DAG dag, Configuration conf)
  {
    SampleInput input = new SampleInput();
    Collector collector = new Collector();
    StreamFactory.fromInput(input, input.beanOutput, name("input"))
      .addCompositeStreams(new PlaysForWord())
      .print(name("console"))
      .endWith(collector, collector.input, name("Collector"))
      .populateDag(dag);

  }
}
