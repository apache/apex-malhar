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
import java.io.IOException;
import java.io.InputStream;
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
 * @author Pramod Immaneni <pramod@malhar-inc.com>
 * Pramod: Need to implement a better test such as reading from a test rss file
 * and checking the entry contents. Would also need to simulate rss updates.
 */
public class RomeSyndicationOperatorTest {

    static List<RomeFeedEntry> entries;

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
            entries = new ArrayList<RomeFeedEntry>();
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

    public static class CNNSyndicationTestProvider implements RomeStreamProvider {
        private int index;

        public CNNSyndicationTestProvider() {
          index = 0;
        }

        public InputStream getInputStream() throws IOException {
          InputStream is = null;
          if (index == 0) {
            is = getClass().getResourceAsStream("/com/malhartech/contrib/romesyndication/cnn_topstories.rss");
            ++index;
          }else {
            is = getClass().getResourceAsStream("/com/malhartech/contrib/romesyndication/cnn_topstories_updated.rss");
          }
          return is;
        }

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
        //rop.setLocation("http://rss.cnn.com/rss/cnn_topstories.rss");
        rop.setStreamProvider(new CNNSyndicationTestProvider());

        try {
            StramLocalCluster lc = new StramLocalCluster(dag);
            lc.setHeartbeatMonitoringEnabled(false);
            lc.run(10000);

            // TODO review the generated test code and remove the default call to fail.
            //fail("The test case is a prototype.");
            // Check total number
            System.out.println("entries size " + entries.size());
            assert entries.size() == 81;
            // Check first entry
            assert entries.get(0).getSyndEntry().getTitle().equals("Our favorite surprise homecomings");
            assert entries.get(0).getSyndEntry().getUri().equals("http://www.cnn.com/video/#/video/us/2012/09/21/soldier-surprises-daughter-maine.wabi");
            // Check first entry from second run
            assert entries.get(74).getSyndEntry().getTitle().equals("Watch chimney deliver the news");
            assert entries.get(74).getSyndEntry().getUri().equals("http://www.cnn.com/video/#/video/world/2013/03/13/nr-white-smoke-means-new-pope.cnn");
            // Check last entry
            assert entries.get(80).getSyndEntry().getTitle().equals("How the smoke process works");
            assert entries.get(80).getSyndEntry().getUri().equals("http://www.cnn.com/2013/03/12/world/europe/vatican-chapel-stove/index.html");
        } catch ( Exception ex ) {
            Assert.assertFalse(true);
        }
    }
}
