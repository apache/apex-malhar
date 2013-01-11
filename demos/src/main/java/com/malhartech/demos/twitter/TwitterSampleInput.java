/*
 *  Copyright (c) 2012 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.malhartech.demos.twitter;

import com.malhartech.annotation.ShipContainingJars;
import com.malhartech.api.ActivationListener;
import com.malhartech.api.Context.OperatorContext;
import com.malhartech.api.DefaultOutputPort;
import com.malhartech.api.InputOperator;
import com.malhartech.util.CircularBuffer;
import java.util.Properties;
import javax.validation.constraints.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import twitter4j.*;
import twitter4j.conf.ConfigurationBuilder;

/**
 *
 * @author Chetan Narsude <chetan@malhar-inc.com>
 */
@ShipContainingJars(classes = {StatusListener.class, Status.class})
public class TwitterSampleInput implements InputOperator, ActivationListener<OperatorContext>, StatusListener
{
  private static final Logger logger = LoggerFactory.getLogger(TwitterSampleInput.class);
  public final transient DefaultOutputPort<Status> status = new DefaultOutputPort<Status>(this);
  public final transient DefaultOutputPort<String> text = new DefaultOutputPort<String>(this);
  public final transient DefaultOutputPort<String> url = new DefaultOutputPort<String>(this);
  public final transient DefaultOutputPort<?> userMention = null;
  public final transient DefaultOutputPort<?> hashtag = null;
  public final transient DefaultOutputPort<?> media = null;
  /**
   * For tapping into the tweets.
   */
  transient TwitterStream ts;
  transient CircularBuffer<Status> statuses = new CircularBuffer<Status>(1024 * 1024, 10);
  transient int count;
  /**
   * The state which we would like to save for this operator.
   */
  int multiplier;
  private Properties twitterProperties;

  // Following twitter access credentials should be set before using this operator.
  @NotNull
  String consumerKey;
  @NotNull
  String consumerSecret;
  @NotNull
  String accessToken;
  @NotNull
  String accessTokenSecret;

  public String getConsumerKey()
  {
    return consumerKey;
  }

  public void setConsumerKey(String consumerKey)
  {
    this.consumerKey = consumerKey;
  }

  public String getConsumerSecret()
  {
    return consumerSecret;
  }

  public void setConsumerSecret(String consumerSecret)
  {
    this.consumerSecret = consumerSecret;
  }

  public String getAccessToken()
  {
    return accessToken;
  }

  public void setAccessToken(String accessToken)
  {
    this.accessToken = accessToken;
  }

  public String getAccessTokenSecret()
  {
    return accessTokenSecret;
  }

  public void setAccessTokenSecret(String accessTokenSecret)
  {
    this.accessTokenSecret = accessTokenSecret;
  }

  @Override
  public void setup(OperatorContext context)
  {
    if (multiplier != 1) {
      logger.info("Load set to be {}% of the entire twitter feed", multiplier);
    }

    ConfigurationBuilder cb = new ConfigurationBuilder();
    cb.setDebugEnabled(Boolean.valueOf(twitterProperties.getProperty("debug"))).
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
    try {
      for (int i = multiplier; i-- > 0;) {
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

  public void setTwitterProperties(Properties properties)
  {
    twitterProperties = properties;
  }

  public void setFeedMultiplier(int multiplier)
  {
    this.multiplier = multiplier;
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
}
