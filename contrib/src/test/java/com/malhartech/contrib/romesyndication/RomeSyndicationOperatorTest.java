/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package com.malhartech.contrib.romesyndication;

import com.malhartech.api.BaseOperator;
import com.malhartech.api.DAG;
import com.malhartech.api.DefaultInputPort;
import com.malhartech.stram.StramLocalCluster;
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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author Pramod Immaneni <pramod@malhar-inc.com>
 */
public class RomeSyndicationOperatorTest
{
  private static final Logger logger = LoggerFactory.getLogger(RomeSyndicationOperatorTest.class);
  static List<RomeFeedEntry> entries;

  public RomeSyndicationOperatorTest()
  {
  }

  @BeforeClass
  public static void setUpClass()
  {
  }

  @AfterClass
  public static void tearDownClass()
  {
  }

  @Before
  public void setUp()
  {
  }

  @After
  public void tearDown()
  {
  }

  public static class FeedCollector extends BaseOperator
  {
    public FeedCollector()
    {
      entries = new ArrayList<RomeFeedEntry>();
    }

    public final transient DefaultInputPort<RomeFeedEntry> input = new DefaultInputPort<RomeFeedEntry>(this)
    {
      @Override
      public void process(RomeFeedEntry tuple)
      {
        entries.add(tuple);
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

  public static class CNNSyndicationTestProvider implements RomeStreamProvider
  {
    private int index;

    public CNNSyndicationTestProvider()
    {
      index = 0;
    }

    @Override
    public InputStream getInputStream() throws IOException
    {
      InputStream is;
      if (index == 0) {
        is = getClass().getResourceAsStream("/com/malhartech/contrib/romesyndication/cnn_topstories.rss");
        ++index;
      }
      else {
        is = getClass().getResourceAsStream("/com/malhartech/contrib/romesyndication/cnn_topstories_updated.rss");
      }
      return is;
    }

  }

  /**
   * Test of run method, of class RomeSyndicationOperator.
   */
  @Test
  public void testRun()
  {
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
      Assert.assertEquals("Entries size", entries.size(), 81);
      // Check first entry
      Assert.assertEquals("First entry title", entries.get(0).getSyndEntry().getTitle(), "Our favorite surprise homecomings");
      Assert.assertEquals("First entry URI", entries.get(0).getSyndEntry().getUri(), "http://www.cnn.com/video/#/video/us/2012/09/21/soldier-surprises-daughter-maine.wabi");
      // Check first entry from second run
      Assert.assertEquals("Second run first entry title", entries.get(74).getSyndEntry().getTitle(), "Watch chimney deliver the news");
      Assert.assertEquals("Second run first entry URI", entries.get(74).getSyndEntry().getUri(), "http://www.cnn.com/video/#/video/world/2013/03/13/nr-white-smoke-means-new-pope.cnn");
      // Check last entry
      Assert.assertEquals("Last entry title", entries.get(80).getSyndEntry().getTitle(), "How the smoke process works");
      Assert.assertEquals("Last entry URI", entries.get(80).getSyndEntry().getUri(), "http://www.cnn.com/2013/03/12/world/europe/vatican-chapel-stove/index.html");
    }
    catch (Exception ex) {
      logger.error(ex.getMessage());
      Assert.assertFalse(true);
    }
  }

}
