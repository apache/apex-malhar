/*
 * Copyright (c) 2014 DataTorrent, Inc. ALL Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
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
import com.datatorrent.api.Operator;
import com.datatorrent.api.StreamingApplication;
import com.datatorrent.api.annotation.ApplicationAnnotation;
import com.datatorrent.contrib.twitter.TwitterSampleInput;
import com.datatorrent.lib.algo.UniqueCounter;
import com.datatorrent.lib.appdata.schemas.SchemaUtils;
import com.datatorrent.lib.appdata.tabular.AppDataTabularServerMap;
import com.datatorrent.lib.io.PubSubWebSocketAppDataQuery;
import com.datatorrent.lib.io.PubSubWebSocketAppDataResult;
import com.google.common.collect.Maps;
import java.net.URI;
import org.apache.hadoop.conf.Configuration;

import java.util.Map;



/**
 * Twitter Demo Application: <br>
 * This demo application samples random public status from twitter, send to Hashtag
 * extractor. <br>
 * Top 10 Hashtag(s) mentioned in tweets in last 5 mins are displayed on every
 * window count (500ms).<br>
 * <br>
 *
 * Real Time Calculation :<br>
 * This application calculates top 10 Hashtag mentioned in tweets in last 5
 * minutes across a 1% random tweet sampling on a rolling window basis.<br>
 * <br>
 * Before running this application, you need to have a <a href="https://dev.twitter.com/apps">Twitter API account</a>
 * and configure the authentication. For launch from CLI, those go into ~/.dt/dt-site.xml:
 * <pre>
 * {@code
 * <?xml version="1.0" encoding="UTF-8"?>
 * <?xml-stylesheet type="text/xsl" href="configuration.xsl"?>
 * <configuration>
 *
 *   <property> <name>dt.operator.TweetSampler.prop.consumerKey</name>
 *   <value>TBD</value> </property>
 *
 *   <property> <name>dt.operator.TweetSampler.prop.consumerSecret</name>
 *   <value>TBD</value> </property>
 *
 *   <property> <name>dt.operator.TweetSampler.prop.accessToken</name>
 *   <value>TBD</value> </property>
 *
 *   <property> <name>dt.operator.TweetSampler.prop.accessTokenSecret</name>
 *   <value>TBD</value> </property>
 * </configuration>
 * }
 * </pre>
 * Custom Attributes: <br>
 * <b>topCounts operator : <b>
 * <ul>
 * <li>Top Count : 10, number of top unique Hashtag to be reported.</li>
 * <li>Sliding window count : 600, report over last 5 min (600 * .5 / 60 mins)</li>
 * <li>window slide value : 1</li>
 * </ul>
 * <p>
 * Running Java Test or Main app in IDE:
 *
 * <pre>
 * LocalMode.runApp(new Application(), 600000); // 10 min run
 * </pre>
 *
 * Run Success : <br>
 * For successful deployment and run, user should see similar output on console as below:
 *
 * <pre>
 * topHashtags: {"string": "{\"gameinsight\":\"12\",\"android\":\"8\",\"WotaJKT48alay\":\"12\",\"TernyataJKT48CyberItuForoumSampah\":\"8\",\"NHL15Subban\":\"30\",\"JKT48CYBERKeracunanCUKRIK\":\"8\",\"verifydjzoodel\":\"59\",\"androidgames\":\"9\",\"مع_الله\":\"10\",\"Jct48cyberTAIBABI\":\"10\"}"}
 * topHashtags: {"string": "{\"gameinsight\":\"12\",\"android\":\"8\",\"WotaJKT48alay\":\"12\",\"TernyataJKT48CyberItuForoumSampah\":\"8\",\"NHL15Subban\":\"30\",\"JKT48CYBERKeracunanCUKRIK\":\"8\",\"verifydjzoodel\":\"59\",\"androidgames\":\"9\",\"مع_الله\":\"10\",\"Jct48cyberTAIBABI\":\"10\"}"}
 * topHashtags: {"string": "{\"gameinsight\":\"12\",\"android\":\"8\",\"WotaJKT48alay\":\"12\",\"TernyataJKT48CyberItuForoumSampah\":\"8\",\"NHL15Subban\":\"30\",\"JKT48CYBERKeracunanCUKRIK\":\"8\",\"verifydjzoodel\":\"59\",\"androidgames\":\"9\",\"مع_الله\":\"10\",\"Jct48cyberTAIBABI\":\"10\"}"}
 * topHashtags: {"string": "{\"gameinsight\":\"12\",\"android\":\"8\",\"WotaJKT48alay\":\"12\",\"TernyataJKT48CyberItuForoumSampah\":\"8\",\"NHL15Subban\":\"30\",\"JKT48CYBERKeracunanCUKRIK\":\"8\",\"verifydjzoodel\":\"59\",\"androidgames\":\"9\",\"مع_الله\":\"10\",\"Jct48cyberTAIBABI\":\"10\"}"}
 * topHashtags: {"string": "{\"gameinsight\":\"12\",\"android\":\"8\",\"WotaJKT48alay\":\"12\",\"TernyataJKT48CyberItuForoumSampah\":\"8\",\"NHL15Subban\":\"30\",\"JKT48CYBERKeracunanCUKRIK\":\"8\",\"verifydjzoodel\":\"59\",\"androidgames\":\"9\",\"مع_الله\":\"10\",\"Jct48cyberTAIBABI\":\"10\"}"}
 * topHashtags: {"string": "{\"gameinsight\":\"12\",\"android\":\"8\",\"WotaJKT48alay\":\"12\",\"TernyataJKT48CyberItuForoumSampah\":\"8\",\"NHL15Subban\":\"30\",\"JKT48CYBERKeracunanCUKRIK\":\"8\",\"verifydjzoodel\":\"59\",\"androidgames\":\"9\",\"مع_الله\":\"10\",\"Jct48cyberTAIBABI\":\"10\"}"}
 * topHashtags: {"string": "{\"gameinsight\":\"12\",\"android\":\"8\",\"WotaJKT48alay\":\"12\",\"TernyataJKT48CyberItuForoumSampah\":\"8\",\"NHL15Subban\":\"30\",\"JKT48CYBERKeracunanCUKRIK\":\"8\",\"verifydjzoodel\":\"59\",\"androidgames\":\"9\",\"مع_الله\":\"10\",\"Jct48cyberTAIBABI\":\"10\"}"}
 * topHashtags: {"string": "{\"gameinsight\":\"12\",\"android\":\"8\",\"WotaJKT48alay\":\"12\",\"TernyataJKT48CyberItuForoumSampah\":\"8\",\"NHL15Subban\":\"30\",\"JKT48CYBERKeracunanCUKRIK\":\"8\",\"verifydjzoodel\":\"59\",\"androidgames\":\"9\",\"مع_الله\":\"10\",\"Jct48cyberTAIBABI\":\"10\"}"}
 * topHashtags: {"string": "{\"gameinsight\":\"12\",\"android\":\"8\",\"WotaJKT48alay\":\"12\",\"TernyataJKT48CyberItuForoumSampah\":\"8\",\"NHL15Subban\":\"30\",\"JKT48CYBERKeracunanCUKRIK\":\"8\",\"verifydjzoodel\":\"59\",\"androidgames\":\"9\",\"مع_الله\":\"10\",\"Jct48cyberTAIBABI\":\"10\"}"}
 * topHashtags: {"string": "{\"gameinsight\":\"12\",\"android\":\"8\",\"WotaJKT48alay\":\"12\",\"TernyataJKT48CyberItuForoumSampah\":\"8\",\"NHL15Subban\":\"30\",\"JKT48CYBERKeracunanCUKRIK\":\"8\",\"verifydjzoodel\":\"59\",\"androidgames\":\"9\",\"مع_الله\":\"10\",\"Jct48cyberTAIBABI\":\"10\"}"}
 * 2013-06-17 14:38:55,201 [main] INFO  stram.StramLocalCluster run - Application finished.
 * 2013-06-17 14:38:55,201 [container-2] INFO  stram.StramChild processHeartbeatResponse - Received shutdown request
 * </pre>
 *
 * Scaling Options : <br>
 * User can scale application by setting intial partition size > 1 on count
 * unique operator. <br>
 * <br>
 *
 * Application DAG : <br>
 * <img src="doc-files/Application.gif" width=600px > <br>
 * <br>
 *
 * Streaming Window Size : 500ms(default) <br>
 * Operator Details : <br>
 * <ul>
 * <li><b>The twitterFeed operator : </b> This operator samples random public
 * statues from twitter and emits to application. <br>
 * Class : com.datatorrent.demos.twitter.TwitterSampleInput <br>
 * StateFull : No, window count 1 <br>
 * </li>
 * <li><b>The HashtagExtractor operator : </b> This operator extracts Hashtag from
 * random sampled statues from twitter. <br>
 * Class : {@link com.datatorrent.demos.twitter.TwitterStatusHashtagExtractor} <br>
 * StateFull : No, window count 1 <br>
 * </li>
 * <li><b>The uniqueCounter operator : </b> This operator aggregates count for each
 * Hashtag extracted from random samples. <br>
 * Class : {@link com.datatorrent.lib.algo.UniqueCounter} <br>
 * StateFull : No, window count 1 <br>
 * </li>
 * <li><b> The topCounts operator : </b> This operator caluculates top Hashtag in last 1
 * min sliding window count 1. <br>
 * Class : com.datatorrent.lib.algo.WindowedTopCounter <br>
 * StateFull : Yes, sliding window count 120 (1 min) <br>
 * </li>
 * <li><b>The operator Console: </b> This operator just outputs the input tuples
 * to the console (or stdout). <br>
 * </li>
 * </ul>
 *
 * @since 1.0.2
 */
@ApplicationAnnotation(name="TwitterTrendingDemo")
public class TwitterTrendingHashtagsApplication implements StreamingApplication
{
  public static final String TABULAR_SCHEMA = "twitterHashTagDataSchema.json";
  public static final String CONVERSION_SCHEMA = "twitterHashTagConverterSchema.json";

  private final Locality locality = null;

  @Override
  public void populateDAG(DAG dag, Configuration conf)
  {
    Operator.OutputPort<String> queryPort = null;
    Operator.InputPort<String> queryResultPort = null;

    String gatewayAddress = dag.getValue(DAG.GATEWAY_CONNECT_ADDRESS);
    URI uri = URI.create("ws://" + gatewayAddress + "/pubsub");
    //LOG.info("WebSocket with gateway at: {}", gatewayAddress);
    PubSubWebSocketAppDataQuery wsIn = dag.addOperator("Query", new PubSubWebSocketAppDataQuery());
    wsIn.setUri(uri);
    queryPort = wsIn.outputPort;
    PubSubWebSocketAppDataResult wsOut = dag.addOperator("QueryResult", new PubSubWebSocketAppDataResult());
    wsOut.setUri(uri);
    queryResultPort = wsOut.input;

    // Setup the operator to get the data from twitter sample stream injected into the system.
    TwitterSampleInput twitterFeed = new TwitterSampleInput();
    twitterFeed = dag.addOperator("TweetSampler", twitterFeed);

    // Setup a node to count the unique Hashtags within a window.
    UniqueCounter<String> uniqueCounter = dag.addOperator("UniqueHashtagCounter", new UniqueCounter<String>());

    // Get the aggregated Hashtag counts and count them over last 5 mins.
    WindowedTopCounter<String> topCounts = dag.addOperator("TopCounter", new WindowedTopCounter<String>());
    AppDataTabularServerMap tabularServer = dag.addOperator("Tabular Server", new AppDataTabularServerMap());

    Map<String, String> conversionMap = Maps.newHashMap();
    conversionMap.put("hashtag", WindowedTopCounter.FIELD_TYPE);

    String tabularSchema = SchemaUtils.jarResourceFileToString(TABULAR_SCHEMA);

    tabularServer.setTabularSchemaJSON(tabularSchema);
    tabularServer.setTableFieldToMapField(conversionMap);

    topCounts.setTopCount(10);
    topCounts.setSlidingWindowWidth(600);
    topCounts.setDagWindowWidth(1);

    dag.addStream("TwittedHashtags", twitterFeed.hashtag, uniqueCounter.data).setLocality(locality);
    // Count unique Hashtags
    dag.addStream("UniqueHashtagCounts", uniqueCounter.count, topCounts.input);
    dag.addStream("MapProvider", topCounts.output, tabularServer.input);
    // Count top 10
    dag.addStream("TopURLQuery", queryPort, tabularServer.query);
    dag.addStream("TopURLResult", tabularServer.queryResult, queryResultPort);
  }
}
