/**
 * Copyright (c) 2012-2012 Malhar, Inc.
 * All rights reserved.
 */
package com.malhartech.demos.twitter;

import java.net.URI;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;

import com.malhartech.api.ApplicationFactory;
import com.malhartech.api.DAG;
import com.malhartech.api.Operator.InputPort;
import com.malhartech.lib.algo.UniqueCounter;
import com.malhartech.lib.algo.WindowedTopCounter;
import com.malhartech.lib.io.ConsoleOutputOperator;
import com.malhartech.lib.io.PubSubWebSocketOutputOperator;

/**
 * Takes Twitter feed and computes top URLs in sliding window.
 * <p>
 * Before running this application, you need to configure the Twitter authentication.
 * For the CLI, those go into ~/.stram/stram-site.xml:
 * <pre>
 * {@code
 * <?xml version="1.0" encoding="UTF-8"?>
 * <?xml-stylesheet type="text/xsl" href="configuration.xsl"?>
 * <configuration>
 *
 *   <property> <name>stram.operator.TweetSampler.consumerKey</name>
 *   <value>TBD</value> </property>
 *
 *   <property> <name>stram.operator.TweetSampler.consumerSecret</name>
 *   <value>TBD</value> </property>
 *
 *   <property> <name>stram.operator.TweetSampler.accessToken</name>
 *   <value>TBD</value> </property>
 *
 *   <property> <name>stram.operator.TweetSampler.accessTokenSecret</name>
 *   <value>TBD</value> </property>

 * </configuration>
 * }
 * </pre>
 */
public class TwitterTopCounterApplication implements ApplicationFactory
{
  private static final boolean inline = false;

  private InputPort<Object> consoleOutput(DAG dag, String operatorName)
  {
    String daemonAddress = dag.attrValue(DAG.STRAM_DAEMON_ADDRESS, null);
    if (!StringUtils.isEmpty(daemonAddress)) {
      URI uri = URI.create("ws://" + daemonAddress + "/pubsub");
      String topic = "demos.twitter." + operatorName;
      //LOG.info("WebSocket with daemon at: {}", daemonAddress);
      PubSubWebSocketOutputOperator<Object> wsOut = dag.addOperator(operatorName, new PubSubWebSocketOutputOperator<Object>());
      wsOut.setUri(uri);
      wsOut.setTopic(topic);
      return wsOut.input;
    }
    ConsoleOutputOperator operator = dag.addOperator(operatorName, new ConsoleOutputOperator());
    operator.setStringFormat(operatorName + ": %s");
    return operator.input;
  }

  @Override
  public void getApplication(DAG dag, Configuration conf)
  {
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

  }

}
