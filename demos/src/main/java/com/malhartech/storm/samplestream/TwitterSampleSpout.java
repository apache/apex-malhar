/*
 *  Copyright (c) 2012-2013 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.malhartech.storm.samplestream;

import com.malhartech.api.Context.OperatorContext;
import com.malhartech.api.DefaultOutputPort;
import com.malhartech.api.InputOperator;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.slf4j.LoggerFactory;
import twitter4j.*;
import twitter4j.conf.ConfigurationBuilder;

/**
 *
 * @author Zhongjian Wang <zhongjian@malhar-inc.com>
 */
public class TwitterSampleSpout implements InputOperator
{
  transient LinkedBlockingQueue<Status> queue = null;
  TwitterStream _twitterStream;
  String _username;
  String _pwd;
  private static final org.slf4j.Logger logger = LoggerFactory.getLogger(TwitterSampleSpout.class);
  public transient DefaultOutputPort<String> output = new DefaultOutputPort<String>(this);

  public TwitterSampleSpout(){}

  public TwitterSampleSpout(String username, String pwd)
  {
    _username = username;
    _pwd = pwd;
  }

  @Override
  public void emitTuples()
  {
    Status ret = queue.poll();
    if (ret == null) {
      try {
        Thread.sleep(50);
      }
      catch (InterruptedException ex) {
        logger.debug(ex.getMessage());
      }
    }
    else {
      output.emit(ret.toString());
    }
  }

  @Override
  public void beginWindow(long windowId)
  {
  }

  @Override
  public void endWindow()
  {
  }

  @Override
  public void setup(OperatorContext context)
  {
    queue = new LinkedBlockingQueue<Status>(1000);
    StatusListener listener = new StatusListener()
    {
      @Override
      public void onStatus(Status status)
      {
        queue.offer(status);
      }

      @Override
      public void onDeletionNotice(StatusDeletionNotice sdn)
      {
      }

      @Override
      public void onTrackLimitationNotice(int i)
      {
      }

      @Override
      public void onScrubGeo(long l, long l1)
      {
      }

      @Override
      public void onException(Exception excptn)
      {
      }
    };
    TwitterStreamFactory fact = new TwitterStreamFactory(new ConfigurationBuilder().setUser(_username).setPassword(_pwd).build());
    _twitterStream = fact.getInstance();
    _twitterStream.addListener(listener);
    _twitterStream.sample();
  }

  @Override
  public void teardown()
  {
    _twitterStream.shutdown();
  }
}
