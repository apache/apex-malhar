package org.apache.apex.malhar.stream.sample;

import java.util.Arrays;

import org.joda.time.Duration;

import org.apache.apex.malhar.stream.api.WindowedStream;
import org.apache.apex.malhar.stream.api.function.Function;
import org.apache.apex.malhar.stream.api.impl.StreamFactory;
import org.apache.apex.malhar.lib.window.WindowOption;
import org.apache.hadoop.conf.Configuration;

import com.datatorrent.api.DAG;
import com.datatorrent.api.StreamingApplication;
import com.datatorrent.api.annotation.ApplicationAnnotation;

/**
 * An application example with stream api
 */
@ApplicationAnnotation(name = "WordCountStreamingApiDemo")
public class ApplicationWithWindowedStreamAPI implements StreamingApplication
{

  @Override
  public void populateDAG(DAG dag, Configuration configuration)
  {
    String localFolder = "./src/test/resources/data";
    WindowedStream stream = StreamFactory
        .fromFolder(localFolder)
        .flatMap(new Function.FlatMapFunction<String, String>()
        {
          @Override
          public Iterable<String> f(String input)
          {
            return Arrays.asList(input.split(" "));
          }
        })
        .window(new WindowOption.TimeWindows(Duration.standardDays(72).plus(Duration.standardHours(3))).slideBy(Duration.standardHours(4).plus(Duration.standardMinutes(20))));
  }
}
