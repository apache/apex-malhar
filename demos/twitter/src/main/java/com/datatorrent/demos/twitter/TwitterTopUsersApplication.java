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
import com.datatorrent.api.StreamingApplication;
import com.datatorrent.api.annotation.ApplicationAnnotation;
import com.datatorrent.contrib.twitter.TwitterSampleInput;
import com.datatorrent.lib.algo.UniqueCounter;

import org.apache.hadoop.conf.Configuration;

/**
 * This application extracts the top users from twitter
 * {@link com.datatorrent.demos.twitter.TwitterTopUsersApplication} <br>
 * Run Sample :
 *
 * <pre>
 * 2013-06-17 16:50:34,911 [Twitter Stream consumer-1[Establishing connection]] INFO  twitter4j.TwitterStreamImpl info - Connection established.
 * 2013-06-17 16:50:34,912 [Twitter Stream consumer-1[Establishing connection]] INFO  twitter4j.TwitterStreamImpl info - Receiving status stream.
 * {kiryasjoelinfo=1, ash_dans=1, NolanSchlange=1, MadisonPunzo=1, neslisahsen_=1, _DaRealJayee=2, Nash_Malibu=1, peacecompassion=1}
 * </pre>
 *
 * @since 0.3.2
 */
@ApplicationAnnotation(name = TwitterTopUsersApplication.APP_NAME)
public class TwitterTopUsersApplication implements StreamingApplication
{

  public static final String SNAPSHOT_SCHEMA = "twitterUserDataSchema.json";
  public static final String APP_NAME = "TwitterUserDemo";

  @Override
  public void populateDAG(DAG dag, Configuration conf)
  {
    TwitterSampleInput twitterFeed = new TwitterSampleInput();
    twitterFeed = dag.addOperator("TweetSampler", twitterFeed);

    UniqueCounter<String> uniqueCounter = dag.addOperator("UniqueWordCounter",
        new UniqueCounter<String>());
    WindowedTopCounter<String> topCounts = dag.addOperator("TopCounter",
        new WindowedTopCounter<String>());

    topCounts.setSlidingWindowWidth(600);
    topCounts.setDagWindowWidth(1);
    
    dag.addStream("TweetStream", twitterFeed.userMention, uniqueCounter.data);
    dag.addStream("UniqueWordCounts", uniqueCounter.count, topCounts.input);

    TwitterTopCounterApplication.consoleOutput(dag, "topUsers",
        topCounts.output, SNAPSHOT_SCHEMA, "user");
  }
}
