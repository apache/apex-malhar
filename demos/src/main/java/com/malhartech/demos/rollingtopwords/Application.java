/*
 *  Copyright (c) 2012 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.malhartech.demos.rollingtopwords;

import com.malhartech.api.ApplicationFactory;
import com.malhartech.api.DAG;
import com.malhartech.demos.twitter.TwitterSampleInput;
import com.malhartech.lib.algo.UniqueCounter;
import com.malhartech.lib.algo.WindowedTopCounter;
import com.malhartech.lib.io.ConsoleOutputOperator;

import org.apache.hadoop.conf.Configuration;

/**
 * This program will output the top N frequent word from twitter feed
 *
 * @author Zhongjian Wang<zhongjian@malhar-inc.com>
 */
public class Application implements ApplicationFactory
{
  private static final boolean inline = true;
  @Override
  public void getApplication(DAG dag, Configuration conf)
  {
    TwitterSampleInput twitterFeed = dag.addOperator("TweetSampler", TwitterSampleInput.class);

    TwitterStatusWordExtractor wordExtractor = dag.addOperator("WordExtractor", TwitterStatusWordExtractor.class);

    UniqueCounter<String> uniqueCounter = dag.addOperator("UniqueWordCounter", new UniqueCounter<String>());

    WindowedTopCounter<String> topCounts = dag.addOperator("TopCounter", new WindowedTopCounter<String>());
    topCounts.setTopCount(10);
    topCounts.setSlidingWindowWidth(600, 1);

    dag.addStream("TweetStream", twitterFeed.text, wordExtractor.input).setInline(inline);
    dag.addStream("TwittedWords", wordExtractor.output, uniqueCounter.data).setInline(inline);

    dag.addStream("UniqueWordCounts", uniqueCounter.count, topCounts.input).setInline(inline);

    ConsoleOutputOperator consoleOperator = dag.addOperator("topWords", new ConsoleOutputOperator());
    consoleOperator.setStringFormat("topWords: %s");

    dag.addStream("TopWords", topCounts.output, consoleOperator.input).setInline(inline);
  }
}
