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

import java.io.Serializable;

import com.esotericsoftware.kryo.DefaultSerializer;
import com.esotericsoftware.kryo.serializers.JavaSerializer;
import com.sun.syndication.feed.synd.SyndEntry;

import com.datatorrent.lib.codec.KryoJdkContainer;

/**
 * RomeFeedEntry that wraps a Rome syndication entry.<p><br>
 *
 * <br>
 * The Rome SyndEntry needs to be wrapped up with a custom class since it cannot be
 * directly serialized by Kryo as a contained object does not have a default constructor.
 * Also implementing a simpler equals method to make checks for the object in collections faster.<br>
 * <br>
 *
 * @since 0.3.2
 */
@DefaultSerializer(JavaSerializer.class)
public class RomeFeedEntry extends KryoJdkContainer<SyndEntry> implements Serializable
{
  /**
   * Empty constructor.
   */
  public RomeFeedEntry()
  {
  }

  /**
   * Create a RomeFeedEntry using a Rome SyndEntry object.
   *
   * @param syndEntry The Rome SyndEntry object
   */
  public RomeFeedEntry(SyndEntry syndEntry)
  {
    //this.syndEntry = syndEntry;
    super(syndEntry);
  }

  /**
   * Set the Rome SyndEntry object.
   *
   * @param syndEntry The SyndEntry object
   */
  public void setSyndEntry(SyndEntry syndEntry)
  {
    setComponent(syndEntry);
  }

  /**
   * Get the Rome SyndEntry object.
   *
   * @return The SyndEntry object
   */
  public SyndEntry getSyndEntry()
  {
    return getComponent();
  }

  /**
   * Override equals to tell if the given object is equal to this RomeFeedEntry object.
   * Compares the title and uri of the underlying SyndEntrys of both objects to determine equality.
   *
   * @param o The given object
   * @return Whether the given object is equal to this object or not
   */
  @Override
  public boolean equals(Object o)
  {
    boolean equal = false;
    if (o instanceof RomeFeedEntry) {
      RomeFeedEntry rfe = (RomeFeedEntry)o;
      SyndEntry syndEntry = getComponent();
      equal = syndEntry.getTitle().equals(rfe.getComponent().getTitle())
              && syndEntry.getUri().equals(rfe.getComponent().getUri());
    }
    return equal;
  }

}
