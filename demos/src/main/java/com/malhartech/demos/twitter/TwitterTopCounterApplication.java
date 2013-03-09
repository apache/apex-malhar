/**
 * Copyright (c) 2012-2012 Malhar, Inc.
 * All rights reserved.
 */
package com.malhartech.demos.twitter;

import com.malhartech.api.ApplicationFactory;
import com.malhartech.api.DAG;
import com.malhartech.api.Operator.InputPort;
import com.malhartech.lib.algo.UniqueCounter;
import com.malhartech.lib.algo.WindowedTopCounter;
import com.malhartech.lib.io.ConsoleOutputOperator;
import com.malhartech.lib.io.HttpOutputOperator;
import java.net.URI;
import org.apache.hadoop.conf.Configuration;

/**
 * Takes Twitter feed and computes top URLs in sliding window.<p>
 */
public class TwitterTopCounterApplication implements ApplicationFactory
{
  private static final boolean inline = false;

  private InputPort<Object> consoleOutput(DAG dag, String operatorName)
  {
    // hack to output to HTTP based on actual environment
    String serverAddr = System.getenv("MALHAR_AJAXSERVER_ADDRESS");
    if (serverAddr != null) {
      HttpOutputOperator<Object> operator = dag.addOperator(operatorName, new HttpOutputOperator<Object>());
      operator.setResourceURL(URI.create("http://" + serverAddr + "/channel/" + operatorName));
      return operator.input;
    }
    ConsoleOutputOperator operator = dag.addOperator(operatorName, new ConsoleOutputOperator());
    operator.setStringFormat(operatorName + ": %s");
    return operator.input;
  }

  @Override
  public DAG getApplication(Configuration conf)
  {
    DAG dag = new DAG(conf);
    dag.setAttribute(DAG.STRAM_APPNAME, "TwitterDevApplication");

    // Setup the operator to get the data from twitter sample stream injected into the system.
    TwitterSampleInput twitterFeed = dag.addOperator("TweetSampler", TwitterSampleInput.class);

    //  Setup the operator to get the URLs extracted from the twitter statuses
    TwitterStatusURLExtractor urlExtractor = dag.addOperator("URLExtractor", TwitterStatusURLExtractor.class);

    // Setup a node to count the unique urls within a window.
    UniqueCounter<String> uniqueCounter = dag.addOperator("UniqueURLCounter", new UniqueCounter<String>());

    // Get the aggregated url counts and count them over the time frame
    WindowedTopCounter<String> topCounts = dag.addOperator("TopCounter", new WindowedTopCounter<String>());
    topCounts.setTopCount(10);
    topCounts.setSlidingWindowWidth(600, 1);

    // Feed the statuses from feed into the input of the url extractor.
    dag.addStream("TweetStream", twitterFeed.status, urlExtractor.input).setInline(true);
    //  Start counting the urls coming out of URL extractor
    dag.addStream("TwittedURLs", urlExtractor.url, uniqueCounter.data).setInline(inline);
    // Count unique urls
    dag.addStream("UniqueURLCounts", uniqueCounter.count, topCounts.input).setInline(inline);
    // Count top 10
    dag.addStream("TopURLs", topCounts.output, consoleOutput(dag, "topURLs")).setInline(inline);

    return dag;
  }

}
