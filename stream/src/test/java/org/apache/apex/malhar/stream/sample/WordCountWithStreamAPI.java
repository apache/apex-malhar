package org.apache.apex.malhar.stream.sample;

import java.util.Arrays;

import org.joda.time.Duration;

import org.apache.apex.malhar.lib.window.TriggerOption;
import org.apache.apex.malhar.lib.window.Tuple;
import org.apache.apex.malhar.lib.window.WindowOption;
import org.apache.apex.malhar.stream.api.ApexStream;
import org.apache.apex.malhar.stream.api.function.Function;
import org.apache.apex.malhar.stream.api.impl.StreamFactory;
import org.apache.hadoop.conf.Configuration;

import com.datatorrent.api.DAG;
import com.datatorrent.api.StreamingApplication;
import com.datatorrent.api.annotation.ApplicationAnnotation;
import com.datatorrent.lib.util.KeyValPair;

/**
 * Created by siyuan on 6/27/16.
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
        .millis(1000)).accumulatingFiredPanes()).countByKey(new Function.MapFunction<String, Tuple<KeyValPair<String, Long>>>()
    {
      @Override
      public Tuple<KeyValPair<String, Long>> f(String input)
      {
        return new Tuple.PlainTuple(new KeyValPair<>(input, 1l));
      }
    }).print();
    stream.populateDag(dag);
  }
}