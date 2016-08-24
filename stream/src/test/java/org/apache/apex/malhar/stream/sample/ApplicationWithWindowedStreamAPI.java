package org.apache.apex.malhar.stream.sample;

import java.util.Arrays;

import org.apache.apex.malhar.stream.api.ApexStream;
import org.apache.apex.malhar.stream.api.WindowedStream;
import org.apache.apex.malhar.stream.api.function.Function;
import org.apache.apex.malhar.stream.api.impl.StreamFactory;
import org.apache.hadoop.conf.Configuration;

import com.datatorrent.api.DAG;
import com.datatorrent.api.StreamingApplication;
import com.datatorrent.api.annotation.ApplicationAnnotation;
import com.datatorrent.lib.window.Quantification.TimeUnit;

import static com.datatorrent.lib.window.WindowOption.WindowOptionBuilder.intoEvery;

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
        .window(intoEvery(10, TimeUnit.WEEK).and(2, TimeUnit.DAY).and(3, TimeUnit.HOUR).slideBy(4, TimeUnit.HOUR).and(20, TimeUnit.MINUTE));
  }
}
