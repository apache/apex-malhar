/*
 *  Copyright (c) 2012 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.malhartech.demos.twitter;

import com.malhartech.annotation.ShipContainingJars;
import com.malhartech.api.DefaultOutputPort;
import com.malhartech.api.OperatorConfiguration;
import com.malhartech.api.SyncInputOperator;
import com.malhartech.dag.OperatorContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import twitter4j.*;
import twitter4j.conf.ConfigurationBuilder;

/**
 *
 * @author Chetan Narsude <chetan@malhar-inc.com>
 */
@ShipContainingJars(classes = {StatusListener.class, Status.class})
public class TwitterSyncSampleInput extends TwitterSampleInput implements SyncInputOperator
{
  private static final Logger logger = LoggerFactory.getLogger(TwitterSyncSampleInput.class);

  @Override
  public Runnable getDataPoller()
  {
    return new Runnable()
    {
      @Override
      public synchronized void run()
      {
        try {
          this.wait();
        }
        catch (InterruptedException ex) {
          logger.info("{} exiting generation of the input since got interrupted with {}", this, ex);
        }
      }
    };
  }
}
