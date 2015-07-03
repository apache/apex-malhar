/**
 * Copyright (C) 2015 DataTorrent, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.datatorrent.demos.twitter;


import com.datatorrent.api.DAG;
import com.datatorrent.api.DAG.Locality;
import com.datatorrent.api.StreamingApplication;
import com.datatorrent.api.annotation.ApplicationAnnotation;
import com.datatorrent.contrib.twitter.TwitterSampleInput;
import com.datatorrent.lib.algo.UniqueCounter;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;


/**
 * This application is same as other twitter demo
 * {@link com.datatorrent.demos.twitter.TwitterTopCounterApplication} <br>
 * Run Sample :
 *
 * <pre>
 * 2013-06-17 16:50:34,911 [Twitter Stream consumer-1[Establishing connection]] INFO  twitter4j.TwitterStreamImpl info - Connection established.
 * 2013-06-17 16:50:34,912 [Twitter Stream consumer-1[Establishing connection]] INFO  twitter4j.TwitterStreamImpl info - Receiving status stream.
 * topWords: {}
 * topWords: {love=1, ate=1, catch=1, calma=1, Phillies=1, ela=1, from=1, running=1}
 * </pre>
 *
 * @since 0.3.2
 */
@ApplicationAnnotation(name=TwitterTopWordsApplication.APP_NAME)
public class TwitterTopWordsApplication implements StreamingApplication
{
  public static final String SNAPSHOT_SCHEMA = "twitterWordDataSchema.json";
  public static final String CONVERSION_SCHEMA = "twitterWordConverterSchema.json";
  public static final String APP_NAME = "RollingTopWordsDemo";
  public static final String PROP_USE_APPDATA = "dt.application." + APP_NAME + ".useAppData";

  @Override
  public void populateDAG(DAG dag, Configuration conf)
  {
    TwitterSampleInput twitterFeed = new TwitterSampleInput();
    twitterFeed = dag.addOperator("TweetSampler", twitterFeed);

    TwitterStatusWordExtractor wordExtractor = dag.addOperator("WordExtractor", TwitterStatusWordExtractor.class);
    UniqueCounter<String> uniqueCounter = dag.addOperator("UniqueWordCounter", new UniqueCounter<String>());
    WindowedTopCounter<String> topCounts = dag.addOperator("TopCounter", new WindowedTopCounter<String>());

    topCounts.setSlidingWindowWidth(120);
    topCounts.setDagWindowWidth(1);

    dag.addStream("TweetStream", twitterFeed.text, wordExtractor.input);
    dag.addStream("TwittedWords", wordExtractor.output, uniqueCounter.data);
    dag.addStream("UniqueWordCounts", uniqueCounter.count, topCounts.input).setLocality(Locality.CONTAINER_LOCAL);

    TwitterTopCounterApplication.consoleOutput(dag, "topWords", topCounts.output, !StringUtils.isEmpty(conf.get(PROP_USE_APPDATA)), SNAPSHOT_SCHEMA, "word");
  }
}
