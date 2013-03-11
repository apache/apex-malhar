/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package com.malhartech.contrib.romesyndication;

import com.malhartech.contrib.romesyndication.RomeFeedEntry;
import com.malhartech.contrib.romesyndication.RomeSyndicationOperator;
import com.malhartech.api.BaseOperator;
import com.malhartech.api.DAG;
import com.malhartech.api.DAGContext;
import com.malhartech.api.DefaultInputPort;
import com.malhartech.api.Sink;
import com.malhartech.api.StreamCodec;
import com.malhartech.lib.codec.JavaSerializationStreamCodec;
import com.malhartech.stram.StramLocalCluster;
import com.malhartech.util.AttributeMap;
import com.malhartech.util.AttributeMap.DefaultAttributeMap;
import com.sun.syndication.feed.synd.SyndEntry;
import java.util.ArrayList;
import java.util.List;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import static org.junit.Assert.*;

/**
 *
 * @author pramod
 * Pramod: Need to implement a better test such as reading from a test rss file
 * and checking the entry contents. Would also need to simulate rss updates.
 */
public class RomeSyndicationOperatorTest {

    static List entries;
    
    public RomeSyndicationOperatorTest() {
    }
    
    @BeforeClass
    public static void setUpClass() {
    }
    
    @AfterClass
    public static void tearDownClass() {
    }
    
    @Before
    public void setUp() {
    }
    
    @After
    public void tearDown() {
    }

    public static class FeedCollector extends BaseOperator {  
        
        public FeedCollector() {
            entries = new ArrayList();
        }
        
        public final transient DefaultInputPort<RomeFeedEntry> input = new DefaultInputPort<RomeFeedEntry>(this) {
            public void process(RomeFeedEntry tuple) {
                entries.add( tuple );
                SyndEntry syndEntry = tuple.getSyndEntry();
                System.out.println( syndEntry.getTitle() + " " + syndEntry.getUri() );
            }
            
            /*
            @Override
            public Class<? extends StreamCodec<SyndEntry>> getStreamCodec() {
                Class c = JavaSerializationStreamCodec.class;                
                return (Class<? extends StreamCodec<SyndEntry>>)c;
            }
            */
        
        };
        
    }
    
    /**
     * Test of run method, of class RomeSyndicationOperator.
     */
    @Test
    public void testRun() {
        System.out.println("run");
        
        DAG dag = new DAG();
        RomeSyndicationOperator rop = dag.addOperator("romesyndication", RomeSyndicationOperator.class);
        FeedCollector fc = dag.addOperator("feedcollector", FeedCollector.class);
        dag.addStream("ss", rop.outputPort, fc.input);
        
        rop.setInterval(2000);
        rop.setLocation("http://rss.cnn.com/rss/cnn_topstories.rss");
        
        try {
            StramLocalCluster lc = new StramLocalCluster(dag);
            lc.setHeartbeatMonitoringEnabled(false);
            lc.run(4000);

            // TODO review the generated test code and remove the default call to fail.
            //fail("The test case is a prototype.");
            assert entries.size() > 0;
        } catch ( Exception ex ) {
            Assert.assertFalse(true);
        }
    }
}
