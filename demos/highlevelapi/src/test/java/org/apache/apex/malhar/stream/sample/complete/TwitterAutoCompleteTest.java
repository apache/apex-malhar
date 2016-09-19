/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.apex.malhar.stream.sample.complete;

import org.junit.Ignore;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.conf.Configuration;

import com.datatorrent.api.LocalMode;

/**
 * Testing the TwitterAutoComplete Application. In order to run this test, you need to create an app
 * at https://apps.twitter.com, then generate your consumer and access keys and tokens, and set the following properties
 * for the application before running it:
 * Your application consumer key,
 * Your application consumer secret,
 * Your twitter access token, and
 * Your twitter access token secret.
 *
 * This test is mainly for local demonstration purpose. Default time to run the application is 1 minute, please
 * set the time you need to run the application before you run.
 */
public class TwitterAutoCompleteTest
{
  private static final Logger logger = LoggerFactory.getLogger(org.apache.apex.malhar.stream.sample.complete.AutoCompleteTest.class);

  @Test
  @Ignore
  public void TwitterAutoCompleteTest() throws Exception
  {
    LocalMode lma = LocalMode.newInstance();
    Configuration conf = new Configuration(false);
    //uncomment the following lines and change YOUR_XXX to the corresponding information needed.
    //conf.set("dt.application.TwitterAutoComplete.operator.tweetSampler.consumerKey", "YOUR_CONSUMERKEY");
    //conf.set("dt.application.TwitterAutoComplete.operator.tweetSampler.consumerSecret", "YOUR_CONSUERSECRET");
    //conf.set("dt.application.TwitterAutoComplete.operator.tweetSampler.accessToken", "YOUR_ACCESSTOKEN");
    //conf.set("dt.application.TwitterAutoComplete.operator.tweetSampler.accessTokenSecret", "YOUR_TOKENSECRET");
    lma.prepareDAG(new TwitterAutoComplete(), conf);
    LocalMode.Controller lc = lma.getController();
    long start = System.currentTimeMillis();
    lc.run(60000); // Set your desired time to run the application here.
    long end = System.currentTimeMillis();
    long time = end - start;
    logger.info("Test used " + time + " ms");
  }

}
