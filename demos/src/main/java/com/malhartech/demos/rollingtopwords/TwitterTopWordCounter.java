/**
 * Copyright (c) 2012-2012 Malhar, Inc.
 * All rights reserved.
 */
package com.malhartech.demos.rollingtopwords;

import com.malhartech.api.DAG;
import com.malhartech.api.Operator.InputPort;
import com.malhartech.demos.twitter.TwitterSampleInput;
import com.malhartech.lib.algo.UniqueCounter;
import com.malhartech.lib.algo.WindowedTopCounter;
import com.malhartech.lib.io.ConsoleOutputOperator;
import com.malhartech.lib.io.HttpOutputOperator;
import java.net.URI;
import org.apache.hadoop.conf.Configuration;

/**
 * In this demo, it will output the most frequent words with the number of appearance in the last time window from the text of twitter sample stream
 *
 * @author Zhongjian Wang<zhongjian@malhar-inc.com>
 */
public class TwitterTopWordCounter extends DAG
{
  private static final boolean inline = false;
  private static final long serialVersionUID = 201211201543L;

  private InputPort<Object> consoleOutput(String nodeName)
  {
    // hack to output to HTTP based on actual environment
    String serverAddr = System.getenv("MALHAR_AJAXSERVER_ADDRESS");
    if (serverAddr != null) {
      HttpOutputOperator<Object> operator = addOperator(nodeName, new HttpOutputOperator<Object>());
      operator.setResourceURL(URI.create("http://" + serverAddr + "/channel/" + nodeName));
      return operator.input;
    }

    ConsoleOutputOperator operator = addOperator(nodeName, new ConsoleOutputOperator());
    operator.setStringFormat(nodeName + ": %s");
    return operator.input;
  }

  public TwitterTopWordCounter(Configuration conf)
  {
    super(conf);
    // Setup the operator to get the data from twitter sample stream injected into the system.
    TwitterSampleInput twitterFeed = addOperator("TweetSampler", TwitterSampleInput.class);

    //  Setup the operator to get the Words extracted from the twitter statuses
    TwitterStatusWordExtractor wordExtractor = addOperator("WordExtractor", TwitterStatusWordExtractor.class);

    // Setup a node to count the unique urls within a window.
    UniqueCounter<String> uniqueCounter = addOperator("UniqueWordCounter", new UniqueCounter<String>());

    // Get the aggregated url counts and count them over the timeframe
    WindowedTopCounter<String> topCounts = addOperator("TopCounter", new WindowedTopCounter<String>());
    topCounts.setTopCount(10);
    topCounts.setSlidingWindowWidth(600, 1);

    // Feed the statuses from feed into the input of the word extractor.
    addStream("TweetStream", twitterFeed.status, wordExtractor.input).setInline(true);
    //  Start counting the words coming out of Word extractor
    addStream("TwittedWords", wordExtractor.output, uniqueCounter.data).setInline(inline);
    // Count unique words
    addStream("UniqueWordCounts", uniqueCounter.count, topCounts.input).setInline(inline);
    // Count top 10
    addStream("TopWords", topCounts.output, consoleOutput("topWords")).setInline(inline);
  }
}
