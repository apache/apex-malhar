/*
 *  Copyright (c) 2012 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.datatorrent.demos.rollingtopwords;

import com.datatorrent.api.StreamingApplication;
import com.datatorrent.api.DAG;
import com.datatorrent.demos.twitter.TwitterSampleInput;
import com.datatorrent.lib.algo.UniqueCounter;
import com.datatorrent.lib.algo.WindowedTopCounter;
import com.datatorrent.lib.io.ConsoleOutputOperator;

import org.apache.hadoop.conf.Configuration;

/**
 * This program will output the top N frequent word from twitter feed
 *
 * @author Zhongjian Wang<zhongjian@malhar-inc.com>
 */
public class Application implements StreamingApplication
{
  private static final boolean inline = true;
  @Override
  public void populateDAG(DAG dag, Configuration conf)
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
