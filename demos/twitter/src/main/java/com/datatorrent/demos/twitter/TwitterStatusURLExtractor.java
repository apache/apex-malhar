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
package com.datatorrent.demos.twitter;

import com.datatorrent.common.util.BaseOperator;
import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.api.DefaultOutputPort;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import twitter4j.Status;
import twitter4j.URLEntity;

/**
 * <p>TwitterStatusURLExtractor class.</p>
 *
 * @since 0.3.2
 */
public class TwitterStatusURLExtractor extends BaseOperator
{
  public final transient DefaultOutputPort<String> url = new DefaultOutputPort<String>();
  public final transient DefaultInputPort<Status> input = new DefaultInputPort<Status>()
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

  private static final Logger LOG = LoggerFactory.getLogger(TwitterStatusURLExtractor.class);
}
