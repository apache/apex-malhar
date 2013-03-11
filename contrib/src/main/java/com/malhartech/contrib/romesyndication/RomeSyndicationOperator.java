/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package com.malhartech.contrib.romesyndication;

import com.malhartech.api.InputOperator;
import com.malhartech.lib.io.SimpleSinglePortInputOperator;
import com.sun.syndication.feed.synd.SyndContent;
import com.sun.syndication.feed.synd.SyndContentImpl;
import com.sun.syndication.feed.synd.SyndEntry;
import com.sun.syndication.feed.synd.SyndEntryImpl;
import com.sun.syndication.feed.synd.SyndFeed;
import com.sun.syndication.io.SyndFeedInput;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author pramod
 * This class provides a news syndication operator that uses rome library to 
 * parse the syndication feeds. Rome can parse most syndication formats including
 * RSS and Atom. The location of the feed is specified to the operator. The
 * operator spawns a thread that will poll the syndication source location.
 * The poll interval can also be specified. When the operator encounters new
 * syndication entries it emits them through the default output port.
 */
public class RomeSyndicationOperator extends SimpleSinglePortInputOperator<RomeFeedEntry> implements Runnable {
    
    private static final Logger logger = LoggerFactory.getLogger(RomeSyndicationOperator.class);
    private String location;
    private int interval;
    private List<RomeFeedEntry> feedItems;
    
    public RomeSyndicationOperator() {
        feedItems = new ArrayList<RomeFeedEntry>();
    }
    
    /**
     * Set the syndication feed location
     * @param location The syndication feed location
     */
    public void setLocation( String location ) {
        this.location = location;
    }
    
    /**
     * Get the syndication feed location
     * @return The syndication feed location
     */
    public String getLocation() {
        return location;
    }
    
    /**
     * Set the syndication feed poll interval
     * @param interval The poll interval
     */
    public void setInterval( int interval ) {
        this.interval = interval;
    }
    
    /**
     * Get the syndication feed poll interval
     * @return The poll interval 
     */
    public int getInterval() {
        return interval;
    }
    

    @Override
    /**
     * Thread processing of the syndication feeds
     */
    public void run() {
        try {
            while ( true ) {
                try {
                    URL url = new URL( location );
                    SyndFeedInput feedInput = new SyndFeedInput();
                    InputStream ips = url.openStream();
                    SyndFeed feed = feedInput.build(new InputStreamReader(ips));
                    List entries = feed.getEntries();
                    List<RomeFeedEntry> nfeedItems = new ArrayList<RomeFeedEntry>();
                    for ( int i = 0; i < entries.size(); ++i ) {
                        SyndEntry syndEntry = (SyndEntry)entries.get( i );
                        RomeFeedEntry romeFeedEntry = getSerializableEntry(syndEntry);
                        if (!feedItems.contains(romeFeedEntry)) {                            
                            outputPort.emit(romeFeedEntry);
                        }
                        nfeedItems.add(romeFeedEntry);
                    }
                    feedItems = nfeedItems;
                } catch ( Exception e ){
                    logger.error(e.getMessage());
                }
                Thread.sleep(interval);
            }
        } catch ( InterruptedException ie ) {
            logger.error( "Interrupted: " + ie.getMessage() );
        }
    }
    
    /**
     * Get a serializable syndEntry for the given syndEntry.
     * Not all implementations of syndEntry are serializable according to rome
     * documentation. This method creates and returns a copy of the original
     * syndEntry that is java serializable.
     * @param syndEntry The syndEntry to create a serializable copy of
     * @return The serializable copy syndEntry
     */
    private RomeFeedEntry getSerializableEntry(SyndEntry syndEntry) {
        SyndEntry serSyndEntry = new SyndEntryImpl();
        serSyndEntry.copyFrom(syndEntry);
        //return serSyndEntry;
        RomeFeedEntry romeFeedEntry = new RomeFeedEntry(serSyndEntry);
        return romeFeedEntry;
    }
    
}
