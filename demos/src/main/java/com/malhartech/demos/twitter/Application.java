/**
 * Copyright (c) 2012-2012 Malhar, Inc.
 * All rights reserved.
 */
package com.malhartech.demos.twitter;

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

  @Override
  public DAG getApplication(Configuration conf)
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

    DAG b = new DAG(conf);
    /*
     * Setup the operator to get the data from twitter sample stream injected into the system.
     */
    Operator twitterFeed = b.addOperator("TweetSampler", TwitterSampleInput.class);
    twitterFeed.setProperty("FeedMultiplier", String.valueOf(100));
    for (Entry<Object, Object> property: properties.entrySet()) {
      String key = propertyBase.concat(".").concat(property.getKey().toString());
//      logger.info("remove this: setting property {} = {}", key, property.getValue().toString());
      twitterFeed.setProperty(key, property.getValue().toString());
    }

    /*
     * Setup the operator to get the URLs extracted from the twitter statuses.
     */
    Operator urlExtractor = b.addOperator("URLExtractor", TwitterStatusURLExtractor.class);

    /*
     * Feed the statuses from feed into the input of the url extractor.
     */
    b.addStream("tweetStream")
            .setSource(twitterFeed.getOutput(TwitterSampleInput.OPORT_STATUS))
            .addSink(urlExtractor.getInput(TwitterStatusURLExtractor.INPUT))
            .setInline(true);

    /*
     * Let's setup a node to count the unique urls within a window.
     */
    Operator uniqueCounter = b.addOperator("UniqueURLCounter", UniqueCounter.class);

    /*
     * Start counting the urls coming out of URL extractor
     */
    b.addStream("twittedURLs")
            .setSource(urlExtractor.getOutput(TwitterStatusURLExtractor.OUTPUT))
            .addSink(uniqueCounter.getInput(UniqueCounter.INPUT))
            .setInline(inline);

    /*
     * Get the aggregated url counts and count them over the timeframe.
     */
    Operator topCounts = b.addOperator("TopCounter", WindowedTopCounter.class);
    topCounts.setProperty("topCount", String.valueOf(10));

    b.addStream("uniqueURLCounts")
            .setSource(uniqueCounter.getOutput(UniqueCounter.OUTPUT))
            .addSink(topCounts.getInput(WindowedTopCounter.INPUT))
            .setInline(inline);

    b.addStream("TopURLs")
            .setSource(topCounts.getOutput(WindowedTopCounter.OUTPUT))
            .addSink(consoleOutput(b, "topURLs"))
            .setInline(inline);

    // these settings only affect distributed mode
    b.getConf().setInt(DAG.STRAM_CONTAINER_MEMORY_MB, 512);
    b.getConf().setInt(DAG.STRAM_MASTER_MEMORY_MB, 512);

    return b;
  }
}
