/*
 * Copyright (c) 2013 DataTorrent, Inc. ALL Rights Reserved.
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

import java.util.concurrent.ArrayBlockingQueue;

import javax.validation.constraints.Min;
import javax.validation.constraints.NotNull;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import twitter4j.*;
import twitter4j.conf.ConfigurationBuilder;

import com.datatorrent.api.ActivationListener;
import com.datatorrent.api.Context.OperatorContext;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.api.InputOperator;
import com.datatorrent.api.annotation.ShipContainingJars;

/**
 * Read input from Twitter. <p> <br>
 *
 * This test can only be run from command line using command line interface script.
 * You need to set twitter authentication credentials in $HOME/.stram/stram-site.xml file in order to run this.
 * The authentication requires following 4 information.
 *   Your application consumer key,
 *   Your application consumer secret,
 *   Your twitter access token, and
 *   Your twitter access token secret.
 *
 * @since 0.3.2
 */
@ShipContainingJars(classes = {StatusListener.class, Status.class})
public class TwitterSampleInput implements InputOperator, ActivationListener<OperatorContext>, StatusListener
{
  private static final Logger logger = LoggerFactory.getLogger(TwitterSampleInput.class);
  public final transient DefaultOutputPort<Status> status = new DefaultOutputPort<Status>();
  public final transient DefaultOutputPort<String> text = new DefaultOutputPort<String>();
  public final transient DefaultOutputPort<String> url = new DefaultOutputPort<String>();
  public final transient DefaultOutputPort<?> userMention = null;
  public final transient DefaultOutputPort<?> hashtag = null;
  public final transient DefaultOutputPort<?> media = null;
  /**
   * Enable debugging.
   */
  private boolean debug;
  /**
   * For tapping into the tweets.
   */
  transient TwitterStream ts;
  private transient ArrayBlockingQueue<Status> statuses = new ArrayBlockingQueue<Status>(1024 * 1024);
  transient int count;
  /**
   * The state which we would like to save for this operator.
   */
  private int multiplier = 1;
  @Min(0)
  private int multiplierVariance=0;

  /* Following twitter access credentials should be set before using this operator. */
  @NotNull
  private String consumerKey;
  @NotNull
  private String consumerSecret;
  @NotNull
  private String accessToken;
  @NotNull
  private String accessTokenSecret;

  @Override
  public void setup(OperatorContext context)
  {
    if (multiplier != 1) {
      logger.info("Load set to be {}% of the entire twitter feed", multiplier);
    }

    ConfigurationBuilder cb = new ConfigurationBuilder();
    cb.setDebugEnabled(debug).
            setOAuthConsumerKey(consumerKey).
            setOAuthConsumerSecret(consumerSecret).
            setOAuthAccessToken(accessToken).
            setOAuthAccessTokenSecret(accessTokenSecret);

    ts = new TwitterStreamFactory(cb.build()).getInstance();
  }

  @Override
  public void teardown()
  {
    ts = null;
  }

  @Override
  public void onStatus(Status status)
  {
    int randomMultiplier=multiplier;

    if(multiplierVariance>0){
      int min = multiplier - multiplierVariance;
      if(min<0) min=0;

      int max = multiplier + multiplierVariance;
      randomMultiplier= min + (int)(Math.random() * ((max-min)+1));
    }
    try {
      for (int i = randomMultiplier; i-- > 0;) {
        statuses.put(status);
        count++;
      }
    }
    catch (InterruptedException ex) {
    }
  }

  @Override
  public void endWindow()
  {
    if (count % 16 == 0) {
      logger.debug("processed {} statuses", count);
    }
  }

  @Override
  public void onDeletionNotice(StatusDeletionNotice statusDeletionNotice)
  {
    // do nothing
  }

  @Override
  public void onTrackLimitationNotice(int numberOfLimitedStatuses)
  {
    // do nothing
  }

  @Override
  public void onScrubGeo(long userId, long upToStatusId)
  {
    // do nothing
  }

  @Override
  public void onException(Exception excptn)
  {
    logger.info("Stopping samping because {}", excptn.getLocalizedMessage());
    synchronized (this) {
      this.notifyAll();
    }
  }

  @Override
  public void beginWindow(long windowId)
  {
  }

  @Override
  public void activate(OperatorContext context)
  {
    ts.addListener(this);
    // we can only listen to tweets containing links by callng ts.links().
    // it seems it requires prior signed agreement with twitter.
    ts.sample();
  }

  @Override
  public void deactivate()
  {
    ts.shutdown();
  }

  public void setFeedMultiplier(int multiplier)
  {
    this.multiplier = multiplier;
  }

  public void setFeedMultiplierVariance(int multiplierVariance){
    this.multiplierVariance = multiplierVariance;
  }

  @Override
  public void emitTuples()
  {
    for (int size = statuses.size(); size-- > 0;) {
      Status s = statuses.poll();
      if (status.isConnected()) {
        status.emit(s);
      }

      if (text.isConnected()) {
        text.emit(s.getText());
      }

      if (url.isConnected()) {
        URLEntity[] entities = s.getURLEntities();
        if (entities != null) {
          for (URLEntity ue: entities) {
            url.emit((ue.getExpandedURL() == null ? ue.getURL() : ue.getExpandedURL()).toString());
          }
        }
      }
      // do the same thing for all the other output ports.
    }
  }

  /**
   * @param debug the debug to set
   */
  public void setDebug(boolean debug)
  {
    this.debug = debug;
  }

  /**
   * @param consumerKey the consumerKey to set
   */
  public void setConsumerKey(String consumerKey)
  {
    this.consumerKey = consumerKey;
  }

  /**
   * @param consumerSecret the consumerSecret to set
   */
  public void setConsumerSecret(String consumerSecret)
  {
    this.consumerSecret = consumerSecret;
  }

  /**
   * @param accessToken the accessToken to set
   */
  public void setAccessToken(String accessToken)
  {
    this.accessToken = accessToken;
  }

  /**
   * @param accessTokenSecret the accessTokenSecret to set
   */
  public void setAccessTokenSecret(String accessTokenSecret)
  {
    this.accessTokenSecret = accessTokenSecret;
  }

}
