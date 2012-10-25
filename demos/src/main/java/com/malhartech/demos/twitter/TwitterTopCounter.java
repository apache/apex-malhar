/**
 * Copyright (c) 2012-2012 Malhar, Inc.
 * All rights reserved.
 */
package com.malhartech.demos.twitter;

import com.malhartech.api.DAG;
import com.malhartech.api.Operator.InputPort;
import com.malhartech.lib.algo.UniqueCounter;
import com.malhartech.lib.algo.WindowedTopCounter;
import com.malhartech.lib.io.ConsoleOutputOperator;
import com.malhartech.lib.io.HttpOutputOperator;
import java.io.IOException;
import java.net.URI;
import java.util.Properties;
import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Example of application configuration in Java.<p>
 */
public class TwitterTopCounter extends DAG
{
  private static final Logger logger = LoggerFactory.getLogger(TwitterTopCounter.class);
  private static final boolean inline = false;

  private InputPort<Object> consoleOutput(String nodeName)
  {
    // hack to output to HTTP based on actual environment
    String serverAddr = System.getenv("MALHAR_AJAXSERVER_ADDRESS");
    if (serverAddr != null) {
      HttpOutputOperator<Object> operator = addOperator(nodeName, HttpOutputOperator.class);
      operator.setResourceURL(URI.create("http://" + serverAddr + "/channel/" + nodeName));
      return operator.input;
    }

    ConsoleOutputOperator<Object> operator = addOperator(nodeName, ConsoleOutputOperator.class);
    operator.setStringFormat(nodeName + ": %s");
    return operator.input;
  }

  public final TwitterSampleInput getTwitterFeed(String name, boolean sync, int multby)
  {
    final String propertyBase = "twitter4j";
    Properties properties = new Properties();
    try {
      properties.load(this.getClass().getResourceAsStream(propertyBase.concat(".properties")));
    }
    catch (IOException e) {
      logger.error("Could not read the much needed credentials file because of {}", e.getLocalizedMessage());
      return null;
    }
    /*
     * Setup the operator to get the data from twitter sample stream injected into the system.
     */
    logger.info("Creating {}synchronous instance of TwitterSampleInput", sync? "": "a");
    TwitterSampleInput oper = addOperator(name, sync? TwitterSyncSampleInput.class: TwitterAsyncSampleInput.class);
    oper.setTwitterProperties(properties);
    oper.setFeedMultiplier(multby);
    return oper;
  }

  public final TwitterStatusURLExtractor getTwitterUrlExtractor(String name)
  {
    TwitterStatusURLExtractor oper = addOperator(name, TwitterStatusURLExtractor.class);
    return oper;
  }

  public final UniqueCounter getUniqueCounter(String name)
  {
    UniqueCounter oper = addOperator(name, UniqueCounter.class);
    return oper;
  }

  public final WindowedTopCounter getTopCounter(String name, int count)
  {
    WindowedTopCounter oper = addOperator(name, WindowedTopCounter.class);
    oper.setTopCount(count);
    return oper;
  }

  public TwitterTopCounter(Configuration conf, boolean sync)
  {
    super(conf);

    TwitterSampleInput twitterFeed = getTwitterFeed("TweetSampler", sync, 100); // Setup the operator to get the data from twitter sample stream injected into the system.
    TwitterStatusURLExtractor urlExtractor = getTwitterUrlExtractor("URLExtractor"); //  Setup the operator to get the URLs extracted from the twitter statuses
    UniqueCounter uniqueCounter = getUniqueCounter("UniqueURLCounter"); // Setup a node to count the unique urls within a window.
    WindowedTopCounter topCounts = getTopCounter("TopCounter", 10);  // Get the aggregated url counts and count them over the timeframe

    // Feed the statuses from feed into the input of the url extractor.
    addStream("TweetStream", twitterFeed.status, urlExtractor.input).setInline(true);
    //  Start counting the urls coming out of URL extractor
    addStream("TwittedURLs", urlExtractor.url, uniqueCounter.data).setInline(inline);
    // Count unique urls
    addStream("UniqueURLCounts", uniqueCounter.count, topCounts.input).setInline(inline);
    // Count top 10
    addStream("TopURLs", topCounts.output, consoleOutput("topURLs")).setInline(inline);
  }
}
