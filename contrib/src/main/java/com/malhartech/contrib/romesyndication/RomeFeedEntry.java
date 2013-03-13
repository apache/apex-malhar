/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package com.malhartech.contrib.romesyndication;

import com.esotericsoftware.kryo.DefaultSerializer;
import com.sun.syndication.feed.synd.SyndEntry;

/**
 * RomeFeedEntry that wraps a Rome syndication entry.<p><br>
 *
 * <br>
 * Need to wrap Rome SyndEntry with a custom class since the default serializer
 * in Kryo needs an object and all its contained objects to have a default constructor
 * which is not the case for SyndEntry. The solution is to create a custom wrapper
 * class and implement a custom kryo serializer.<br>
 * <br>
 *
 * @author Pramod Immaneni <pramod@malhar-inc.com>
 */
@DefaultSerializer(RomeFeedEntrySerializer.class)
public class RomeFeedEntry {

    private SyndEntry syndEntry;

    /**
     * Empty constructor.
     */
    public RomeFeedEntry() {

    }

    /**
     * Create a RomeFeedEntry using a Rome SyndEntry object.
     * @param syndEntry The Rome SyndEntry object
     */
    public RomeFeedEntry(SyndEntry syndEntry) {
        this.syndEntry = syndEntry;
    }

    /**
     * Set the Rome SyndEntry object.
     * @param syndEntry The SyndEntry object
     */
    public void setSyndEntry(SyndEntry syndEntry) {
        this.syndEntry = syndEntry;
    }

    /**
     * Get the Rome SyndEntry object.
     * @return The SyndEntry object
     */
    public SyndEntry getSyndEntry() {
        return syndEntry;
    }

    /**
     * Override equals to tell if the given object is equal to this RomeFeedEntry object.
     * Compares the title and uri of the underlying SyndEntrys of both objects to determine equality.
     * @param o The given object
     * @return Whether the given object is equal to this object or not
     */
    @Override
    public boolean equals(Object o) {
        boolean equal = false;
        if (o instanceof RomeFeedEntry) {
            RomeFeedEntry rfe = (RomeFeedEntry)o;
            equal = syndEntry.getTitle().equals(rfe.getSyndEntry().getTitle())
                          && syndEntry.getUri().equals(rfe.getSyndEntry().getUri());
        }
        return equal;
    }

}
