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

import java.net.URI;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;

import com.datatorrent.api.Context;
import com.datatorrent.api.DAG;
import com.datatorrent.api.DAG.Locality;
import com.datatorrent.api.Operator;
import com.datatorrent.api.Operator.OutputPort;
import com.datatorrent.api.StreamingApplication;
import com.datatorrent.api.annotation.ApplicationAnnotation;

import com.datatorrent.contrib.twitter.TwitterSampleInput;
import com.datatorrent.lib.algo.UniqueCounter;
import com.datatorrent.lib.appdata.schemas.SchemaUtils;
import com.datatorrent.lib.appdata.snapshot.AppDataSnapshotServerMap;
import com.datatorrent.lib.io.PubSubWebSocketAppDataQuery;
import com.datatorrent.lib.io.PubSubWebSocketAppDataResult;
import com.datatorrent.lib.io.ConsoleOutputOperator;
import com.datatorrent.lib.io.PubSubWebSocketOutputOperator;

import com.google.common.collect.Maps;

import java.util.List;
import java.util.Map;
/**
 * Twitter Demo Application: <br>
 * This demo application samples random public status from twitter, send to url
 * extractor. <br>
 * Top 10 url(s) mentioned in tweets in last 5 mins are displayed on every
 * window count (500ms).<br>
 * <br>
 *
 * Real Time Calculation :<br>
 * This application calculates top 10 url mentioned in tweets in last 5
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
 *   <property> <name>dt.operator.TweetSampler.consumerKey</name>
 *   <value>TBD</value> </property>
 *
 *   <property> <name>dt.operator.TweetSampler.consumerSecret</name>
 *   <value>TBD</value> </property>
 *
 *   <property> <name>dt.operator.TweetSampler.accessToken</name>
 *   <value>TBD</value> </property>
 *
 *   <property> <name>dt.operator.TweetSampler.accessTokenSecret</name>
 *   <value>TBD</value> </property>
 * </configuration>
 * }
 * </pre>
 * Custom Attributes: <br>
 * <b>topCounts operator : <b>
 * <ul>
 * <li>Top Count : 10, number of top unique url to be reported.</li>
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
 * For successful deployment and run, user should see following output on
 * console:
 *
 * <pre>
 * topURLs: {http://goo.gl/V0R05=2, http://etsy.me/10r1Yg3=6, http://tinyurl.com/88b5jqb=2, http://www.justunfollow.com=4, http://fllwrs.com=2, http://goo.gl/a9Sjp=2, http://goo.gl/iKeVH=2, http://Unfollowers.me=7, http://freetexthost.com/j3y03la4g3=2, http://uranaitter.com=4}
 * topURLs: {http://goo.gl/V0R05=2, http://etsy.me/10r1Yg3=6, http://tinyurl.com/88b5jqb=2, http://www.justunfollow.com=4, http://fllwrs.com=2, http://goo.gl/a9Sjp=2, http://goo.gl/iKeVH=2, http://Unfollowers.me=7, http://freetexthost.com/j3y03la4g3=2, http://uranaitter.com=4}
 * topURLs: {http://goo.gl/V0R05=2, http://etsy.me/10r1Yg3=6, http://tinyurl.com/88b5jqb=2, http://www.justunfollow.com=4, http://fllwrs.com=2, http://goo.gl/a9Sjp=2, http://goo.gl/iKeVH=2, http://Unfollowers.me=7, http://freetexthost.com/j3y03la4g3=2, http://uranaitter.com=4}
 * topURLs: {http://goo.gl/V0R05=2, http://etsy.me/10r1Yg3=6, http://tinyurl.com/88b5jqb=2, http://www.justunfollow.com=4, http://fllwrs.com=2, http://goo.gl/a9Sjp=2, http://goo.gl/iKeVH=2, http://Unfollowers.me=7, http://freetexthost.com/j3y03la4g3=2, http://uranaitter.com=4}
 * topURLs: {http://goo.gl/V0R05=2, http://etsy.me/10r1Yg3=6, http://tinyurl.com/88b5jqb=2, http://www.justunfollow.com=4, http://fllwrs.com=2, http://goo.gl/a9Sjp=2, http://goo.gl/iKeVH=2, http://Unfollowers.me=7, http://freetexthost.com/j3y03la4g3=2, http://uranaitter.com=4}
 * topURLs: {http://goo.gl/V0R05=2, http://etsy.me/10r1Yg3=6, http://tinyurl.com/88b5jqb=2, http://www.justunfollow.com=4, http://fllwrs.com=2, http://goo.gl/a9Sjp=2, http://goo.gl/iKeVH=2, http://Unfollowers.me=7, http://freetexthost.com/j3y03la4g3=2, http://uranaitter.com=4}
 * topURLs: {http://goo.gl/V0R05=2, http://etsy.me/10r1Yg3=6, http://tinyurl.com/88b5jqb=2, http://www.justunfollow.com=4, http://fllwrs.com=2, http://goo.gl/a9Sjp=2, http://goo.gl/iKeVH=2, http://Unfollowers.me=7, http://freetexthost.com/j3y03la4g3=2, http://uranaitter.com=4}
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
 * <li><b>The urlExtractor operator : </b> This operator extracts url from
 * random sampled statues from twitter. <br>
 * Class : {@link com.datatorrent.demos.twitter.TwitterStatusURLExtractor} <br>
 * StateFull : No, window count 1 <br>
 * </li>
 * <li><b>The uniqueCounter operator : </b> This operator aggregates count for each
 * url extracted from random samples. <br>
 * Class : {@link com.datatorrent.lib.algo.UniqueCounter} <br>
 * StateFull : No, window count 1 <br>
 * </li>
 * <li><b> The topCounts operator : </b> This operator caluculates top url in last 1
 * min sliding window count 1. <br>
 * Class : com.datatorrent.lib.algo.WindowedTopCounter <br>
 * StateFull : Yes, sliding window count 120 (1 min) <br>
 * </li>
 * <li><b>The operator Console: </b> This operator just outputs the input tuples
 * to the console (or stdout). <br>
 * </li>
 * </ul>
 *
 * @since 0.3.2
 */
@ApplicationAnnotation(name=TwitterTopCounterApplication.APP_NAME)
public class TwitterTopCounterApplication implements StreamingApplication
{
  public static final String SNAPSHOT_SCHEMA = "twitterURLDataSchema.json";
  public static final String CONVERSION_SCHEMA = "twitterURLConverterSchema.json";
  public static final String APP_NAME = "TwitterDemo";
  public static final String PROP_USE_APPDATA = "dt.application." + APP_NAME + ".useAppData";

  private final Locality locality = null;

  @Override
  public void populateDAG(DAG dag, Configuration conf)
  {
    // Setup the operator to get the data from twitter sample stream injected into the system.
    TwitterSampleInput twitterFeed = new TwitterSampleInput();
    twitterFeed = dag.addOperator("TweetSampler", twitterFeed);

    //  Setup the operator to get the URLs extracted from the twitter statuses
    TwitterStatusURLExtractor urlExtractor = dag.addOperator("URLExtractor", TwitterStatusURLExtractor.class);

    // Setup a node to count the unique urls within a window.
    UniqueCounter<String> uniqueCounter = dag.addOperator("UniqueURLCounter", new UniqueCounter<String>());
    // Get the aggregated url counts and count them over last 5 mins.
    dag.setAttribute(uniqueCounter, Context.OperatorContext.APPLICATION_WINDOW_COUNT, 600);
    dag.setAttribute(uniqueCounter, Context.OperatorContext.SLIDE_BY_WINDOW_COUNT, 1);


    WindowedTopCounter<String> topCounts = dag.addOperator("TopCounter", new WindowedTopCounter<String>());
    topCounts.setTopCount(10);
    topCounts.setSlidingWindowWidth(1);
    topCounts.setDagWindowWidth(1);

    // Feed the statuses from feed into the input of the url extractor.
    dag.addStream("TweetStream", twitterFeed.status, urlExtractor.input).setLocality(Locality.CONTAINER_LOCAL);
    //  Start counting the urls coming out of URL extractor
    dag.addStream("TwittedURLs", urlExtractor.url, uniqueCounter.data).setLocality(locality);
    // Count unique urls
    dag.addStream("UniqueURLCounts", uniqueCounter.count, topCounts.input);

    consoleOutput(dag, "topURLs", topCounts.output, !StringUtils.isEmpty(conf.get(PROP_USE_APPDATA)), SNAPSHOT_SCHEMA, "url");
  }

  public static void consoleOutput(DAG dag,
                                   String operatorName,
                                   OutputPort<List<Map<String, Object>>> topCount,
                                   boolean useAppData,
                                   String schemaFile,
                                   String alias)
  {
    String gatewayAddress = dag.getValue(DAG.GATEWAY_CONNECT_ADDRESS);
    if (!StringUtils.isEmpty(gatewayAddress)) {
      URI uri = URI.create("ws://" + gatewayAddress + "/pubsub");

      if(!useAppData) {
        String topic = "demos.twitter." + operatorName;
        PubSubWebSocketOutputOperator<Object> wsOut = dag.addOperator(operatorName, new PubSubWebSocketOutputOperator<Object>());
        wsOut.setUri(uri);
        wsOut.setTopic(topic);
        dag.addStream("MapProvider", topCount, wsOut.input);
      }
      else {
        AppDataSnapshotServerMap snapshotServer = dag.addOperator("Snapshot Server", new AppDataSnapshotServerMap());

        Map<String, String> conversionMap = Maps.newHashMap();
        conversionMap.put(alias, WindowedTopCounter.FIELD_TYPE);
        String snapshotServerJSON = SchemaUtils.jarResourceFileToString(schemaFile);

        snapshotServer.setSnapshotSchemaJSON(snapshotServerJSON);
        snapshotServer.setTableFieldToMapField(conversionMap);

        PubSubWebSocketAppDataQuery wsQuery = dag.addOperator("Query", new PubSubWebSocketAppDataQuery());
        wsQuery.setUri(uri);
        Operator.OutputPort<String> queryPort = wsQuery.outputPort;
        PubSubWebSocketAppDataResult wsResult = dag.addOperator("QueryResult", new PubSubWebSocketAppDataResult());
        wsResult.setUri(uri);
        Operator.InputPort<String> queryResultPort = wsResult.input;

        dag.addStream("MapProvider", topCount, snapshotServer.input);
        dag.addStream("Query", queryPort, snapshotServer.query);
        dag.addStream("Result", snapshotServer.queryResult, queryResultPort);
      }
    }
    else {
      ConsoleOutputOperator operator = dag.addOperator(operatorName, new ConsoleOutputOperator());
      operator.setStringFormat(operatorName + ": %s");

      dag.addStream("MapProvider", topCount, operator.input);
    }
  }
}
