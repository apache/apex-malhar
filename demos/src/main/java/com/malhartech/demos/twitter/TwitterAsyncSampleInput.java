/*
 *  Copyright (c) 2012 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.malhartech.demos.twitter;

import com.malhartech.api.AsyncInputOperator;
import twitter4j.Status;
import twitter4j.URLEntity;

/**
 *
 * @author Chetan Narsude <chetan@malhar-inc.com>
 */
public class TwitterAsyncSampleInput extends TwitterSampleInput implements AsyncInputOperator
{
  @Override
  public void injectTuples(long windowId)
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
