/*
 *  Copyright (c) 2012-2013 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.malhartech.contrib.romesyndication;

import com.esotericsoftware.kryo.DefaultSerializer;
import com.malhartech.util.KryoJdkContainer;
import com.malhartech.codec.KryoJdkSerializer;
import com.sun.syndication.feed.synd.SyndEntry;
import java.io.Serializable;

/**
 * RomeFeedEntry that wraps a Rome syndication entry.<p><br>
 *
 * <br>
 * The Rome SyndEntry needs to be wrapped up with a custom class since it cannot be
 * directly serialized by Kryo as a contained object does not have a default constructor.
 * Also implementing a simpler equals method to make checks for the object in collections faster.<br>
 * <br>
 *
 * @author Pramod Immaneni <pramod@malhar-inc.com>
 */
@DefaultSerializer(KryoJdkSerializer.class)
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
