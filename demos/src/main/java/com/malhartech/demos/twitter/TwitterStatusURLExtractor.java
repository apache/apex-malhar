/*
 *  Copyright (c) 2012 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.malhartech.demos.twitter;

import com.malhartech.api.BaseOperator;
import com.malhartech.api.DefaultInputPort;
import com.malhartech.api.DefaultOutputPort;
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
          url.emit((ue.getExpandedURL() == null ? ue.getURL() : ue.getExpandedURL()).toString());
        }
      }
    }
  };
}
