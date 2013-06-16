/*
 *  Copyright (c) 2012 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.datatorrent.demos.twitter;

import com.datatorrent.api.BaseOperator;
import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.api.DefaultOutputPort;

import twitter4j.Status;
import twitter4j.URLEntity;

/**
 *
 * @author Chetan Narsude <chetan@malhar-inc.com>
 */
public class TwitterStatusURLExtractor extends BaseOperator
{
  public final transient DefaultOutputPort<String> url = new DefaultOutputPort<String>(this);
  public final transient DefaultInputPort<Status> input = new DefaultInputPort<Status>(this)
  {
    @Override
    public void process(Status status)
    {
      URLEntity[] entities = status.getURLEntities();
      if (entities != null) {
        for (URLEntity ue: entities) {
          if (ue != null) { // see why we intermittently get NPEs
            url.emit((ue.getExpandedURL() == null ? ue.getURL() : ue.getExpandedURL()).toString());
          }
        }
      }
    }
  };
}
