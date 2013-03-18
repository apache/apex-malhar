/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package com.malhartech.contrib.romesyndication;

import com.esotericsoftware.kryo.DefaultSerializer;
import com.malhartech.lib.util.KryoJavaContainer;
import com.malhartech.lib.util.KryoJavaSerializer;
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
@DefaultSerializer(KryoJavaSerializer.class)
public class RomeFeedEntry extends KryoJavaContainer<SyndEntry> implements Serializable {

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
        //this.syndEntry = syndEntry;
        super(syndEntry);
    }

    /**
     * Set the Rome SyndEntry object.
     * @param syndEntry The SyndEntry object
     */
    public void setSyndEntry(SyndEntry syndEntry) {
        setMember(syndEntry);
    }

    /**
     * Get the Rome SyndEntry object.
     * @return The SyndEntry object
     */
    public SyndEntry getSyndEntry() {
        return getMember();
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
            SyndEntry syndEntry = getMember();
            equal = syndEntry.getTitle().equals(rfe.getMember().getTitle())
                          && syndEntry.getUri().equals(rfe.getMember().getUri());
        }
        return equal;
    }

}
