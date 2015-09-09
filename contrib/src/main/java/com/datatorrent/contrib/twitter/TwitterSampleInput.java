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
package com.datatorrent.contrib.twitter;

import java.util.concurrent.ArrayBlockingQueue;

import javax.validation.constraints.Min;
import javax.validation.constraints.NotNull;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import twitter4j.*;
import twitter4j.conf.ConfigurationBuilder;

import com.datatorrent.api.Context.OperatorContext;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.api.InputOperator;
import com.datatorrent.api.Operator.ActivationListener;

/**
 * This is an input operator for Twitter.
 * <p>
 * This test can only be run from command line using command line interface script.
 * You need to set twitter authentication credentials in $HOME/.dt/dt-site.xml file in order to run this.
 * The authentication requires following 4 information.
 * Your application consumer key,
 * Your application consumer secret,
 * Your twitter access token, and
 * Your twitter access token secret.
 * </p>
 * @displayName Twitter Input
 * @category Web
 * @tags input operator
 * @since 0.3.2
 */
public class TwitterSampleInput implements InputOperator, ActivationListener<OperatorContext>, StatusListener
{
  /**
   * This is the output port on which the twitter status information is emitted.
   */
  public final transient DefaultOutputPort<Status> status = new DefaultOutputPort<Status>();
  /**
   * This is the output port on which the twitter text is emitted.
   */
  public final transient DefaultOutputPort<String> text = new DefaultOutputPort<String>();
  /**
   * This is the output port on which the twitter url is emitted.
   */
  public final transient DefaultOutputPort<String> url = new DefaultOutputPort<String>();
  /**
   * This is the output port on which the twitter hashtags are emitted.
   */
  public final transient DefaultOutputPort<String> hashtag = new DefaultOutputPort<String>();

  /* the following 3 ports are not implemented so far */
  public final transient DefaultOutputPort<?> userMention = null;
  public final transient DefaultOutputPort<?> media = null;
  /**
   * Enable debugging.
   */
  private boolean debug;
  /**
   * For tapping into the tweets.
   */
  private transient Thread operatorThread;
  private transient TwitterStream ts;
  private transient ArrayBlockingQueue<Status> statuses = new ArrayBlockingQueue<Status>(1024 * 1024);
  transient int count;
  /**
   * The state which we would like to save for this operator.
   */
  private int feedMultiplier = 1;
  @Min(0)
  private int feedMultiplierVariance = 0;

  /* Following twitter access credentials should be set before using this operator. */
  @NotNull
  private String consumerKey;
  @NotNull
  private String consumerSecret;
  @NotNull
  private String accessToken;
  @NotNull
  private String accessTokenSecret;
  /* If twitter connection breaks then do we need to reconnect or exit */
  private boolean reConnect;

  @Override
  public void setup(OperatorContext context)
  {
    operatorThread = Thread.currentThread();

    if (feedMultiplier != 1) {
      logger.info("Load set to be {}% of the entire twitter feed", feedMultiplier);
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
    int randomMultiplier = feedMultiplier;

    if (feedMultiplierVariance > 0) {
      int min = feedMultiplier - feedMultiplierVariance;
      if (min < 0) {
        min = 0;
      }

      int max = feedMultiplier + feedMultiplierVariance;
      randomMultiplier = min + (int)(Math.random() * ((max - min) + 1));
    }
    try {
      for (int i = randomMultiplier; i-- > 0;) {
        statuses.put(status);
        count++;
      }
    }
    catch (InterruptedException ex) {
      logger.debug("Streaming interrupted; Passing the inerruption to the operator", ex);
      operatorThread.interrupt();
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
  public void onException(Exception ex)
  {
    logger.error("Sampling Error", ex);
    logger.debug("reconnect: {}", reConnect);
    ts.shutdown();
    if (reConnect) {
      try {
        Thread.sleep(1000);
      }
      catch (Exception e) {
      }
      setUpTwitterConnection();
    }
    else {
      operatorThread.interrupt();
    }
  }

  private void setUpTwitterConnection()
  {
    ConfigurationBuilder cb = new ConfigurationBuilder();
    cb.setDebugEnabled(debug).
            setOAuthConsumerKey(consumerKey).
            setOAuthConsumerSecret(consumerSecret).
            setOAuthAccessToken(accessToken).
            setOAuthAccessTokenSecret(accessTokenSecret);

    ts = new TwitterStreamFactory(cb.build()).getInstance();
    ts.addListener(TwitterSampleInput.this);
    // we can only listen to tweets containing links by callng ts.links().
    // it seems it requires prior signed agreement with twitter.
    ts.sample();
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
    this.feedMultiplier = multiplier;
  }

  public int getFeedMultiplier()
  {
    return this.feedMultiplier;
  }

  public void setFeedMultiplierVariance(int multiplierVariance)
  {
    this.feedMultiplierVariance = multiplierVariance;
  }

  public int getFeedMultiplierVariance()
  {
    return this.feedMultiplierVariance;
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
          for (URLEntity ue : entities) {
            url.emit((ue.getExpandedURL() == null ? ue.getURL() : ue.getExpandedURL()).toString());
          }
        }
      }

      if (hashtag.isConnected()) {
        HashtagEntity[] hashtagEntities = s.getHashtagEntities();
        if (hashtagEntities != null) {
          for (HashtagEntity he : hashtagEntities) {
            hashtag.emit(he.getText());
          }
        }
      }
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
   * @return the consumerKey
   */
  public String getConsumerKey()
  {
    return consumerKey;
  }

  /**
   * @param consumerKey the consumerKey to set
   */
  public void setConsumerKey(String consumerKey)
  {
    this.consumerKey = consumerKey;
  }

  /**
   * @return the consumerSecret
   */
  public String getConsumerSecret()
  {
    return consumerSecret;
  }

  /**
   * @param consumerSecret the consumerSecret to set
   */
  public void setConsumerSecret(String consumerSecret)
  {
    this.consumerSecret = consumerSecret;
  }

  /**
   * @return the accessToken
   */
  public String getAccessToken()
  {
    return accessToken;
  }

  /**
   * @param accessToken the accessToken to set
   */
  public void setAccessToken(String accessToken)
  {
    this.accessToken = accessToken;
  }

  /**
   * @return the accessTokenSecret
   */
  public String getAccessTokenSecret()
  {
    return accessTokenSecret;
  }

  /**
   * @param accessTokenSecret the accessTokenSecret to set
   */
  public void setAccessTokenSecret(String accessTokenSecret)
  {
    this.accessTokenSecret = accessTokenSecret;
  }

  public boolean isReConnect()
  {
    return reConnect;
  }

  public void setReConnect(boolean reConnect)
  {
    this.reConnect = reConnect;
  }

  @Override
  @SuppressWarnings({"ConstantConditions"})
  public int hashCode()
  {
    int hash = 7;
    hash = 11 * hash + (this.debug ? 1 : 0);
    hash = 11 * hash + this.feedMultiplier;
    hash = 11 * hash + this.feedMultiplierVariance;
    hash = 11 * hash + (this.consumerKey != null ? this.consumerKey.hashCode() : 0);
    hash = 11 * hash + (this.consumerSecret != null ? this.consumerSecret.hashCode() : 0);
    hash = 11 * hash + (this.accessToken != null ? this.accessToken.hashCode() : 0);
    hash = 11 * hash + (this.accessTokenSecret != null ? this.accessTokenSecret.hashCode() : 0);
    return hash;
  }

  @Override
  @SuppressWarnings({"ConstantConditions"})
  public boolean equals(Object obj)
  {
    if (obj == null) {
      return false;
    }
    if (getClass() != obj.getClass()) {
      return false;
    }
    final TwitterSampleInput other = (TwitterSampleInput) obj;
    if (this.debug != other.debug) {
      return false;
    }
    if (this.feedMultiplier != other.feedMultiplier) {
      return false;
    }
    if (this.feedMultiplierVariance != other.feedMultiplierVariance) {
      return false;
    }
    if ((this.consumerKey == null) ? (other.consumerKey != null) : !this.consumerKey.equals(other.consumerKey)) {
      return false;
    }
    if ((this.consumerSecret == null) ? (other.consumerSecret != null) : !this.consumerSecret.equals(other.consumerSecret)) {
      return false;
    }
    if ((this.accessToken == null) ? (other.accessToken != null) : !this.accessToken.equals(other.accessToken)) {
      return false;
    }
    if ((this.accessTokenSecret == null) ? (other.accessTokenSecret != null) : !this.accessTokenSecret.equals(other.accessTokenSecret)) {
      return false;
    }
    return true;
  }

  @Override
  public String toString()
  {
    return "TwitterSampleInput{debug=" + debug + ", feedMultiplier=" + feedMultiplier + ", feedMultiplierVariance=" + feedMultiplierVariance + ", consumerKey=" + consumerKey + ", consumerSecret=" + consumerSecret + ", accessToken=" + accessToken + ", accessTokenSecret=" + accessTokenSecret + '}';
  }

  private static final Logger logger = LoggerFactory.getLogger(TwitterSampleInput.class);
}
