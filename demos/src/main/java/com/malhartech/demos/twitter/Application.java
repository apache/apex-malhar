/**
 * Copyright (c) 2012-2012 Malhar, Inc.
 * All rights reserved.
 */
package com.malhartech.demos.twitter;

import com.malhartech.lib.algo.WindowedTopCounter;
import java.io.IOException;
import java.util.Map.Entry;
import java.util.Properties;

import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.malhartech.dag.ApplicationFactory;
import com.malhartech.dag.DAG;
import com.malhartech.dag.DAG.Operator;
import com.malhartech.lib.io.ConsoleOutputModule;
import com.malhartech.lib.io.HttpOutputModule;
import com.malhartech.lib.math.UniqueCounter;

/**
 * Example of application configuration in Java.<p>
 */
public class Application implements ApplicationFactory
{
  private static final Logger logger = LoggerFactory.getLogger(Application.class);
  private static final boolean inline = false;

  private DAG.InputPort consoleOutput(DAG b, String nodeName)
  {
    // hack to output to HTTP based on actual environment
    String serverAddr = System.getenv("MALHAR_AJAXSERVER_ADDRESS");
    if (serverAddr != null) {
      return b.addOperator(nodeName, HttpOutputModule.class)
              .setProperty(HttpOutputModule.P_RESOURCE_URL, "http://" + serverAddr + "/channel/" + nodeName)
              .getInput(HttpOutputModule.INPUT);
    }
    return b.addOperator(nodeName, ConsoleOutputModule.class)
            //.setProperty(ConsoleOutputModule.P_DEBUG, "true")
            .setProperty(ConsoleOutputModule.P_STRING_FORMAT, nodeName + ": %s")
            .getInput(ConsoleOutputModule.INPUT);
  }

  public Operator getTwitterFeed(String name, DAG b, int multby)
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
    Operator oper = b.addOperator(name, TwitterSampleInput.class);
    oper.setProperty("FeedMultiplier", String.valueOf(multby));
    for (Entry<Object, Object> property: properties.entrySet()) {
      String key = propertyBase.concat(".").concat(property.getKey().toString());
//      logger.info("remove this: setting property {} = {}", key, property.getValue().toString());
      oper.setProperty(key, property.getValue().toString());
    }
    return oper;
  }

  public Operator getTwitterUrlExtractor(String name, DAG b)
  {
    Operator oper = b.addOperator(name, TwitterStatusURLExtractor.class);
    return oper;
  }

  public Operator getUniqueCounter(String name, DAG b) {
    Operator oper = b.addOperator(name, UniqueCounter.class);
    return oper;
  }

  public Operator getTopCounter(String name, DAG b, int count) {
    Operator oper = b.addOperator(name, WindowedTopCounter.class).setProperty("topCount", String.valueOf(count));
    return oper;
  }

  @Override
  public DAG getApplication(Configuration conf)
  {
    DAG b = new DAG(conf);

    Operator twitterFeed = getTwitterFeed("TweetSampler", b, 100); // Setup the operator to get the data from twitter sample stream injected into the system.
    Operator urlExtractor = getTwitterUrlExtractor("URLExtractor", b); //  Setup the operator to get the URLs extracted from the twitter statuses
    Operator uniqueCounter = getUniqueCounter("UniqueURLCounter", b); // Setup a node to count the unique urls within a window.
    Operator topCounts = getTopCounter("TopCounter", b, 10);  // Get the aggregated url counts and count them over the timeframe

    // Feed the statuses from feed into the input of the url extractor.
    b.addStream("tweetStream").setSource(twitterFeed.getOutput(TwitterSampleInput.OPORT_STATUS))
            .addSink(urlExtractor.getInput(TwitterStatusURLExtractor.INPUT)).setInline(true);
    //  Start counting the urls coming out of URL extractor
    b.addStream("twittedURLs").setSource(urlExtractor.getOutput(TwitterStatusURLExtractor.OUTPUT))
            .addSink(uniqueCounter.getInput(UniqueCounter.INPUT)).setInline(inline);
    // Count unique urls
    b.addStream("uniqueURLCounts").setSource(uniqueCounter.getOutput(UniqueCounter.OUTPUT))
            .addSink(topCounts.getInput(WindowedTopCounter.INPUT)).setInline(inline);
    // Count top 10
    b.addStream("TopURLs").setSource(topCounts.getOutput(WindowedTopCounter.OUTPUT))
            .addSink(consoleOutput(b, "topURLs")).setInline(inline);

    return b;
  }
}
