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

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Random;
import javax.annotation.Nullable;

import org.joda.time.Duration;

import org.apache.apex.malhar.lib.function.Function;
import org.apache.apex.malhar.lib.util.KeyValPair;
import org.apache.apex.malhar.lib.window.TriggerOption;
import org.apache.apex.malhar.lib.window.Tuple;
import org.apache.apex.malhar.lib.window.Window;
import org.apache.apex.malhar.lib.window.WindowOption;
import org.apache.apex.malhar.lib.window.accumulation.TopN;
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
 * Beam's TopWikipediaSessions Example.
 *
 * @since 3.5.0
 */
@ApplicationAnnotation(name = "TopWikipediaSessions")
public class TopWikipediaSessions implements StreamingApplication
{
  /**
   * A generator that outputs a stream of combinations of some users and some randomly generated edit time.
   */
  public static class SessionGen extends BaseOperator implements InputOperator
  {
    private String[] names = new String[]{"user1", "user2", "user3", "user4"};
    public transient DefaultOutputPort<KeyValPair<String, Long>> output = new DefaultOutputPort<>();

    private static final Duration RAND_RANGE = Duration.standardDays(365);
    private Long minTimestamp;
    private long sleepTime;
    private static int tupleCount = 0;

    public static int getTupleCount()
    {
      return tupleCount;
    }

    private String randomName(String[] names)
    {
      int index = new Random().nextInt(names.length);
      return names[index];
    }

    @Override
    public void setup(Context.OperatorContext context)
    {
      super.setup(context);
      tupleCount = 0;
      minTimestamp = System.currentTimeMillis();
      sleepTime = context.getValue(Context.OperatorContext.SPIN_MILLIS);
    }

    @Override
    public void emitTuples()
    {
      long randMillis = (long)(Math.random() * RAND_RANGE.getMillis());
      long randomTimestamp = minTimestamp + randMillis;
      output.emit(new KeyValPair<String, Long>(randomName(names), randomTimestamp));
      tupleCount++;
      try {
        Thread.sleep(sleepTime);
      } catch (InterruptedException e) {
        // Ignore it.
      }
    }
  }

  public static class Collector extends BaseOperator
  {
    private final int resultSize = 5;
    private static List<List<TempWrapper>> result = new ArrayList<>();

    public static List<List<TempWrapper>> getResult()
    {
      return result;
    }

    public final transient DefaultInputPort<Tuple.WindowedTuple<List<TempWrapper>>> input = new DefaultInputPort<Tuple.WindowedTuple<List<TempWrapper>>>()
    {
      @Override
      public void process(Tuple.WindowedTuple<List<TempWrapper>> tuple)
      {
        if (result.size() == resultSize) {
          result.remove(0);
        }
        result.add(tuple.getValue());
      }
    };
  }


  /**
   * Convert the upstream (user, time) combination to a timestamped tuple of user.
   */
  static class ExtractUserAndTimestamp implements Function.MapFunction<KeyValPair<String, Long>, Tuple.TimestampedTuple<String>>
  {
    @Override
    public Tuple.TimestampedTuple<String> f(KeyValPair<String, Long> input)
    {
      long timestamp = input.getValue();
      String userName = input.getKey();

      // Sets the implicit timestamp field to be used in windowing.
      return new Tuple.TimestampedTuple<>(timestamp, userName);

    }
  }

  /**
   * Computes the number of edits in each user session.  A session is defined as
   * a string of edits where each is separated from the next by less than an hour.
   */
  static class ComputeSessions
      extends CompositeStreamTransform<ApexStream<Tuple.TimestampedTuple<String>>, WindowedStream<Tuple.WindowedTuple<KeyValPair<String, Long>>>>
  {
    @Override
    public WindowedStream<Tuple.WindowedTuple<KeyValPair<String, Long>>> compose(ApexStream<Tuple.TimestampedTuple<String>> inputStream)
    {
      return inputStream

        // Chuck the stream into session windows.
        .window(new WindowOption.SessionWindows(Duration.standardHours(1)), new TriggerOption().accumulatingFiredPanes().withEarlyFiringsAtEvery(1))

        // Count the number of edits for a user within one session.
        .countByKey(new Function.ToKeyValue<Tuple.TimestampedTuple<String>, String, Long>()
        {
          @Override
          public Tuple.TimestampedTuple<KeyValPair<String, Long>> f(Tuple.TimestampedTuple<String> input)
          {
            return new Tuple.TimestampedTuple<KeyValPair<String, Long>>(input.getTimestamp(), new KeyValPair<String, Long>(input.getValue(), 1L));
          }
        }, name("ComputeSessions"));
    }
  }

  /**
   * A comparator class used for comparing two TempWrapper objects.
   */
  public static class Comp implements Comparator<TempWrapper>
  {
    @Override
    public int compare(TempWrapper o1, TempWrapper o2)
    {
      return Long.compare(o1.getValue().getValue(), o2.getValue().getValue());
    }
  }

  /**
   * A function to extract timestamp from a TempWrapper object.
   */
  // TODO: Need to revisit and change back to using TimestampedTuple.
  public static class TimestampExtractor implements com.google.common.base.Function<TempWrapper, Long>
  {
    @Override
    public Long apply(@Nullable TempWrapper input)
    {
      return input.getTimestamp();
    }
  }

  /**
   * A temporary wrapper to wrap a KeyValPair and a timestamp together to represent a timestamped tuple, the reason
   * for this is that we cannot resolve a type conflict when calling accumulate(). After the issue resolved, we can
   * remove this class.
   */
  public static class TempWrapper
  {
    private KeyValPair<String, Long> value;
    private Long timestamp;

    public TempWrapper()
    {

    }

    public TempWrapper(KeyValPair<String, Long> value, Long timestamp)
    {
      this.value = value;
      this.timestamp = timestamp;
    }

    @Override
    public String toString()
    {
      return this.value + "  -  " + this.timestamp;
    }

    public Long getTimestamp()
    {
      return timestamp;
    }

    public void setTimestamp(Long timestamp)
    {
      this.timestamp = timestamp;
    }

    public KeyValPair<String, Long> getValue()
    {
      return value;
    }

    public void setValue(KeyValPair<String, Long> value)
    {
      this.value = value;
    }
  }

  /**
   * Computes the longest session ending in each month, in this case we use 30 days to represent every month.
   */
  private static class TopPerMonth
      extends CompositeStreamTransform<ApexStream<Tuple.WindowedTuple<KeyValPair<String, Long>>>, WindowedStream<Tuple.WindowedTuple<List<TempWrapper>>>>
  {

    @Override
    public WindowedStream<Tuple.WindowedTuple<List<TempWrapper>>> compose(ApexStream<Tuple.WindowedTuple<KeyValPair<String, Long>>> inputStream)
    {
      TopN<TempWrapper> topN = new TopN<>();
      topN.setN(10);
      topN.setComparator(new Comp());

      return inputStream

        // Map the input WindowedTuple to a TempWrapper object.
        .map(new Function.MapFunction<Tuple.WindowedTuple<KeyValPair<String, Long>>, TempWrapper>()
        {
          @Override
          public TempWrapper f(Tuple.WindowedTuple<KeyValPair<String, Long>> input)
          {
            Window window = input.getWindows().iterator().next();
            return new TempWrapper(input.getValue(), window.getBeginTimestamp());
          }
        }, name("TempWrapper"))

        // Apply window and trigger option again, this time chuck the stream into fixed time windows.
        .window(new WindowOption.TimeWindows(Duration.standardDays(30)), new TriggerOption().accumulatingFiredPanes().withEarlyFiringsAtEvery(Duration.standardSeconds(5)))

        // Compute the top 10 user-sessions with most number of edits.
        .accumulate(topN, name("TopN")).with("timestampExtractor", new TimestampExtractor());
    }
  }

  /**
   * A map function that combine the user and his/her edit session together to a string and use that string as a key
   * with number of edits in that session as value to create a new key value pair to send to downstream.
   */
  static class SessionsToStringsDoFn implements Function.MapFunction<Tuple.WindowedTuple<KeyValPair<String, Long>>, Tuple.WindowedTuple<KeyValPair<String, Long>>>
  {
    @Override
    public Tuple.WindowedTuple<KeyValPair<String, Long>> f(Tuple.WindowedTuple<KeyValPair<String, Long>> input)
    {
      Window window = input.getWindows().iterator().next();
      return new Tuple.WindowedTuple<KeyValPair<String, Long>>(window, new KeyValPair<String, Long>(
        input.getValue().getKey()  + " : " + window.getBeginTimestamp() + " : " + window.getDurationMillis(),
        input.getValue().getValue()));
    }
  }

  /**
   * A flatmap function that turns the result into readable format.
   */
  static class FormatOutputDoFn implements Function.FlatMapFunction<Tuple.WindowedTuple<List<TempWrapper>>, String>
  {
    @Override
    public Iterable<String> f(Tuple.WindowedTuple<List<TempWrapper>> input)
    {
      ArrayList<String> result = new ArrayList<>();
      for (TempWrapper item : input.getValue()) {
        String session = item.getValue().getKey();
        long count = item.getValue().getValue();
        Window window = input.getWindows().iterator().next();
        result.add(session + " + " + count + " : " + window.getBeginTimestamp());
      }
      return result;
    }
  }

  /**
   * A composite transform that compute the top wikipedia sessions.
   */
  public static class ComputeTopSessions extends CompositeStreamTransform<ApexStream<KeyValPair<String, Long>>, WindowedStream<Tuple.WindowedTuple<List<TempWrapper>>>>
  {
    @Override
    public WindowedStream<Tuple.WindowedTuple<List<TempWrapper>>> compose(ApexStream<KeyValPair<String, Long>> inputStream)
    {
      return inputStream
        .map(new ExtractUserAndTimestamp(), name("ExtractUserAndTimestamp"))
        .addCompositeStreams(new ComputeSessions())
        .map(new SessionsToStringsDoFn(), name("SessionsToStringsDoFn"))
        .addCompositeStreams(new TopPerMonth());
    }
  }

  @Override
  public void populateDAG(DAG dag, Configuration conf)
  {
    SessionGen sg = new SessionGen();
    Collector collector = new Collector();
    StreamFactory.fromInput(sg, sg.output, name("sessionGen"))
      .addCompositeStreams(new ComputeTopSessions())
      .print(name("console"))
      .endWith(collector, collector.input, name("collector")).populateDag(dag);
  }
}
