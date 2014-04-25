/*
 * Copyright (c) 2014 DataTorrent, Inc. ALL Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.datatorrent.lib.dedup;

import java.io.IOException;
import java.util.Calendar;
import java.util.List;
import java.util.concurrent.Exchanger;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;

import com.datatorrent.api.AttributeMap;
import com.datatorrent.api.DAG;

import com.datatorrent.lib.bucket.Bucket;
import com.datatorrent.lib.bucket.DummyEvent;
import com.datatorrent.lib.bucket.ExpirableHdfsBucketStore;
import com.datatorrent.lib.bucket.TimeBasedBucketManagerImpl;
import com.datatorrent.lib.helper.OperatorContextTestHelper;
import com.datatorrent.lib.testbench.CollectorTestSink;

/**
 * Tests for {@link Deduper}
 */
public class DeduperTest
{
  private static final Logger logger = LoggerFactory.getLogger(DeduperTest.class);

  private final static String APPLICATION_PATH_PREFIX = "target/DeduperTest";
  private final static String APP_ID = "DeduperTest";
  private final static int OPERATOR_ID = 0;

  private final static Exchanger<Long> eventBucketExchanger = new Exchanger<Long>();

  private static class DummyDeduper extends DeduperWithHdfsStore<DummyEvent, DummyEvent>
  {

    @Override
    public void bucketLoaded(Bucket<DummyEvent> bucket)
    {
      try {
        super.bucketLoaded(bucket);
        eventBucketExchanger.exchange(bucket.bucketKey);
      }
      catch (InterruptedException e) {
        throw new RuntimeException(e);
      }
    }

    @Override
    public DummyEvent convert(DummyEvent dummyEvent)
    {
      return dummyEvent;
    }

    public void addEventManuallyToWaiting(DummyEvent event)
    {
      waitingEvents.put(bucketManager.getBucketKeyFor(event), Lists.newArrayList(event));
    }
  }

  private static DummyDeduper deduper;
  private static String applicationPath;
  private static TimeBasedBucketManagerImpl<DummyEvent> storageManager;

  @SuppressWarnings({"rawtypes", "unchecked"})
  @Test
  public void testDedup()
  {
    List<DummyEvent> events = Lists.newArrayList();
    Calendar calendar = Calendar.getInstance();
    for (int i = 0; i < 10; i++) {
      events.add(new DummyEvent(i, calendar.getTimeInMillis()));
    }
    events.add(new DummyEvent(5, calendar.getTimeInMillis()));

    AttributeMap.DefaultAttributeMap attributes = new AttributeMap.DefaultAttributeMap();
    attributes.put(DAG.APPLICATION_ID, APP_ID);
    attributes.put(DAG.APPLICATION_PATH, applicationPath);

    deduper.setup(new OperatorContextTestHelper.TestIdOperatorContext(OPERATOR_ID, attributes));
    CollectorTestSink collectorTestSink = new CollectorTestSink<DummyEvent>();
    deduper.output.setSink(collectorTestSink);

    logger.debug("start round 0");
    deduper.beginWindow(0);
    testRound(events);
    deduper.handleIdleTime();
    deduper.endWindow();
    Assert.assertEquals("output tuples", 10, collectorTestSink.collectedTuples.size());
    collectorTestSink.clear();
    logger.debug("end round 0");

    logger.debug("start round 1");
    deduper.beginWindow(1);
    testRound(events);
    deduper.handleIdleTime();
    deduper.endWindow();
    Assert.assertEquals("output tuples", 0, collectorTestSink.collectedTuples.size());
    collectorTestSink.clear();
    logger.debug("end round 1");

    //Test the sliding window
    try {
      Thread.sleep(1500);
    }
    catch (InterruptedException e) {
      throw new RuntimeException(e);
    }
    deduper.handleIdleTime();
    long now = System.currentTimeMillis();
    for (int i = 10; i < 15; i++) {
      events.add(new DummyEvent(i, now));
    }

    logger.debug("start round 2");
    deduper.beginWindow(2);
    testRound(events);
    deduper.handleIdleTime();
    deduper.endWindow();
    Assert.assertEquals("output tuples", 5, collectorTestSink.collectedTuples.size());
    collectorTestSink.clear();
    logger.debug("end round 2");
    deduper.teardown();
  }

  private void testRound(List<DummyEvent> events)
  {
    for (DummyEvent event : events) {
      deduper.input.process(event);
    }
    try {
      eventBucketExchanger.exchange(null, 1, TimeUnit.SECONDS);
    }
    catch (InterruptedException e) {
      throw new RuntimeException(e);
    }
    catch (TimeoutException e) {
      logger.debug("Timeout Happened");
    }
  }

  @Test
  public void testDeduperRedeploy() throws Exception
  {
    AttributeMap.DefaultAttributeMap attributes = new AttributeMap.DefaultAttributeMap();
    attributes.put(DAG.APPLICATION_ID, APP_ID);
    attributes.put(DAG.APPLICATION_PATH, applicationPath);

    deduper.addEventManuallyToWaiting(new DummyEvent(100, System.currentTimeMillis()));
    deduper.setup(new OperatorContextTestHelper.TestIdOperatorContext(0, attributes));
    eventBucketExchanger.exchange(null, 500, TimeUnit.MILLISECONDS);
    deduper.endWindow();
    deduper.teardown();
  }

  @BeforeClass
  public static void setup()
  {
    applicationPath = OperatorContextTestHelper.getUniqueApplicationPath(APPLICATION_PATH_PREFIX);
    ExpirableHdfsBucketStore<DummyEvent>  bucketStore = new ExpirableHdfsBucketStore<DummyEvent>();
    deduper = new DummyDeduper();
    deduper.setBucketStore(bucketStore);
    storageManager = new TimeBasedBucketManagerImpl<DummyEvent>();
    storageManager.setBucketSpanInMillis(1000);
    storageManager.setMillisPreventingBucketEviction(60000);
    storageManager.initialize(bucketStore);
    deduper.setBucketManager(storageManager);
  }

  @AfterClass
  public static void teardown()
  {
    Path root = new Path(applicationPath);
    try {
      FileSystem fs = FileSystem.newInstance(root.toUri(), new Configuration());
      fs.delete(root, true);
    }
    catch (IOException e) {
      throw new RuntimeException(e);
    }
  }
}
