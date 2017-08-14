/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.apex.malhar.contrib.romesyndication;

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

import com.datatorrent.api.DAG;
import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.api.LocalMode;
import com.datatorrent.common.util.BaseOperator;

/**
 *
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

    public final transient DefaultInputPort<RomeFeedEntry> input = new DefaultInputPort<RomeFeedEntry>()
    {
      @Override
      public void process(RomeFeedEntry tuple)
      {
        entries.add(tuple);
      }
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
        is = getClass().getResourceAsStream("/com/datatorrent/contrib/romesyndication/datatorrent_feed.rss");
        ++index;
      } else {
        is = getClass().getResourceAsStream("/com/datatorrent/contrib/romesyndication/datatorrent_feed_updated.rss");
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
    LocalMode lma = LocalMode.newInstance();
    DAG dag = lma.getDAG();
    RomeSyndicationOperator rop = dag.addOperator("romesyndication", RomeSyndicationOperator.class);
    FeedCollector fc = dag.addOperator("feedcollector", FeedCollector.class);
    dag.addStream("ss", rop.outputPort, fc.input);

    rop.setInterval(2000);
    //rop.setLocation("http://rss.cnn.com/rss/cnn_topstories.rss");
    rop.setStreamProvider(new CNNSyndicationTestProvider());

    try {
      LocalMode.Controller lc = lma.getController();
      lc.setHeartbeatMonitoringEnabled(false);
      lc.run(10000);
      Assert.assertEquals("Entries size", entries.size(), 10);
      // Check first entry
      Assert.assertEquals("First entry title", entries.get(0).getSyndEntry().getTitle(), "Dimensions Computation (Aggregate Navigator) Part 1: Intro");
      Assert.assertEquals("First entry URI", entries.get(0).getSyndEntry().getUri(), "https://www.datatorrent.com/?p=2399");
      // Check first entry from second run
      Assert.assertEquals("Second run first entry title", entries.get(7).getSyndEntry().getTitle(), "Building Applications with Apache Apex and Malhar");
      Assert.assertEquals("Second run first entry URI", entries.get(7).getSyndEntry().getUri(), "https://www.datatorrent.com/?p=2054");
      // Check last entry
      Assert.assertEquals("Last entry title", entries.get(9).getSyndEntry().getTitle(), "Dimensions Computation (Aggregate Navigator) Part 2: Implementation");
      Assert.assertEquals("Last entry URI", entries.get(9).getSyndEntry().getUri(), "https://www.datatorrent.com/?p=2401");
    } catch (Exception ex) {
      logger.error(ex.getMessage());
      Assert.assertFalse(true);
    }
  }

}
