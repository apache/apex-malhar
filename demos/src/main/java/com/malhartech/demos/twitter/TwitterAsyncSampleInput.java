/*
 *  Copyright (c) 2012 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.malhartech.demos.twitter;

import twitter4j.Status;
import twitter4j.URLEntity;

import com.malhartech.api.InputOperator;

/**
 *
 * @author Chetan Narsude <chetan@malhar-inc.com>
 */
public class TwitterAsyncSampleInput extends TwitterSampleInput implements InputOperator
{
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
