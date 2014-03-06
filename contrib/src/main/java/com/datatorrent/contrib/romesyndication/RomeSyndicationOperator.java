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
package com.datatorrent.contrib.romesyndication;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;

import com.sun.syndication.feed.synd.SyndEntry;
import com.sun.syndication.feed.synd.SyndEntryImpl;
import com.sun.syndication.feed.synd.SyndFeed;
import com.sun.syndication.io.SyndFeedInput;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datatorrent.lib.io.SimpleSinglePortInputOperator;

/**
 * Operator for getting syndication feeds processed using Rome.<p><br>
 *
 * <br>
 * This class provides a news syndication operator that uses Rome library to
 * parse the syndication feeds. Rome can parse most syndication formats including
 * RSS and Atom. The location of the feed is specified to the operator. The
 * operator spawns a thread that will poll the syndication source location.
 * The poll interval can also be specified. When the operator encounters new
 * syndication entries it emits them through the default output port.<br>
 *
 * <br>
 *
 * @since 0.3.2
 */
public class RomeSyndicationOperator extends SimpleSinglePortInputOperator<RomeFeedEntry> implements Runnable
{
  private static final Logger logger = LoggerFactory.getLogger(RomeSyndicationOperator.class);
  private String location;
  private RomeStreamProvider streamProvider;
  private int interval;
  private boolean orderedUpdate;
  private List<RomeFeedEntry> feedItems;

  public RomeSyndicationOperator()
  {
    orderedUpdate = false;
    feedItems = new ArrayList<RomeFeedEntry>();
  }

  /**
   * Set the syndication feed location
   *
   * @param location The syndication feed location
   */
  public void setLocation(String location)
  {
    this.location = location;
  }

  /**
   * Get the syndication feed location
   *
   * @return The syndication feed location
   */
  public String getLocation()
  {
    return location;
  }

  /**
   * Set the syndication feed poll interval
   *
   * @param interval The poll interval
   */
  public void setInterval(int interval)
  {
    this.interval = interval;
  }

  /**
   * Get the syndication feed poll interval
   *
   * @return The poll interval
   */
  public int getInterval()
  {
    return interval;
  }

  /**
   * Set the syndication feed provider
   *
   * @param streamProvider The syndication feed provider
   */
  public void setStreamProvider(RomeStreamProvider streamProvider)
  {
    this.streamProvider = streamProvider;
  }

  /**
   * Get the syndication feed provider
   *
   * @return The syndication feed provider
   */
  public RomeStreamProvider getStreamProvider()
  {
    return streamProvider;
  }

  public void setOrderedUpdate(boolean orderedUpdate)
  {
    this.orderedUpdate = orderedUpdate;
  }

  public boolean isOrderedUpdate()
  {
    return orderedUpdate;
  }

  private InputStream getFeedInputStream() throws IOException
  {
    InputStream is;
    if (streamProvider != null) {
      is = streamProvider.getInputStream();
    }
    else {
      URL url = new URL(location);
      is = url.openStream();
    }
    return is;
  }

  @Override
  /**
   * Thread processing of the syndication feeds
   */
  public void run()
  {
    try {
      while (true) {
        InputStreamReader isr = null;
        try {
          isr = new InputStreamReader(getFeedInputStream());
          SyndFeedInput feedInput = new SyndFeedInput();
          SyndFeed feed = feedInput.build(isr);
          List entries = feed.getEntries();
          List<RomeFeedEntry> nfeedItems = new ArrayList<RomeFeedEntry>();
          boolean oldEntries = false;
          for (int i = 0; i < entries.size(); ++i) {
            SyndEntry syndEntry = (SyndEntry)entries.get(i);
            RomeFeedEntry romeFeedEntry = getSerializableEntry(syndEntry);
            if (!oldEntries) {
              if (!feedItems.contains(romeFeedEntry)) {
                outputPort.emit(romeFeedEntry);
              }
              else if (orderedUpdate) {
                oldEntries = true;
              }
            }
            nfeedItems.add(romeFeedEntry);
          }
          feedItems = nfeedItems;
        }
        catch (Exception e) {
          logger.error(e.getMessage());
        }
        finally {
          if (isr != null) {
            try {
              isr.close();
            }
            catch (Exception ce) {
              logger.error(ce.getMessage());
            }
          }
        }
        Thread.sleep(interval);
      }
    }
    catch (InterruptedException ie) {
      logger.error("Interrupted: " + ie.getMessage());
    }
  }

  /**
   * Get a serializable syndEntry for the given syndEntry.
   * Not all implementations of syndEntry are serializable according to rome
   * documentation. This method creates and returns a copy of the original
   * syndEntry that is java serializable.
   *
   * @param syndEntry The syndEntry to create a serializable copy of
   * @return The serializable copy syndEntry
   */
  private RomeFeedEntry getSerializableEntry(SyndEntry syndEntry)
  {
    SyndEntry serSyndEntry = new SyndEntryImpl();
    serSyndEntry.copyFrom(syndEntry);
    //return serSyndEntry;
    RomeFeedEntry romeFeedEntry = new RomeFeedEntry(serSyndEntry);
    return romeFeedEntry;
  }

}
