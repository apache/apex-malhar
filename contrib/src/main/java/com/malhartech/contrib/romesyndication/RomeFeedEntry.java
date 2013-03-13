/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package com.malhartech.contrib.romesyndication;

import com.esotericsoftware.kryo.DefaultSerializer;
import com.sun.syndication.feed.synd.SyndEntry;

/**
 *
 * @author Pramod Immaneni <pramod@malhar-inc.com>
 * Need to wrap rome SyndEntry with a custom class since the default serializer
 * in kryo needs the SyndEntry and all its contained classes to have a default
 * constructor which is not the case. The workaround wass to create a custom
 * wrapper class and implement a custom kryo serializer.
 */
@DefaultSerializer(RomeFeedEntrySerializer.class)
public class RomeFeedEntry {

    private SyndEntry syndEntry;

    public RomeFeedEntry() {

    }

    public RomeFeedEntry(SyndEntry syndEntry) {
        this.syndEntry = syndEntry;
    }

    public void setSyndEntry(SyndEntry syndEntry) {
        this.syndEntry = syndEntry;
    }

    public SyndEntry getSyndEntry() {
        return syndEntry;
    }

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
