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
import com.datatorrent.api.DAG;
import com.datatorrent.lib.bucket.AbstractBucket;
import com.datatorrent.lib.bucket.DummyEvent;
import com.datatorrent.lib.bucket.ExpirableHdfsBucketStore;
import com.datatorrent.lib.bucket.TimeBasedBucketManagerImpl;
import com.datatorrent.lib.helper.OperatorContextTestHelper;
import com.datatorrent.lib.testbench.CollectorTestSink;
import com.datatorrent.lib.util.TestUtils;

/**
 * Tests for {@link Deduper}
 */
public class DeduperManagerTest
{
  private static final Logger logger = LoggerFactory.getLogger(DeduperManagerTest.class);

  private final static String APPLICATION_PATH_PREFIX = "target/DeduperManagerTest";
  private final static String APP_ID = "DeduperManagerTest";
  private final static int OPERATOR_ID = 0;

  private final static Exchanger<Long> eventBucketExchanger = new Exchanger<Long>();

  private static class DummyDeduper extends DeduperWithHdfsStore<DummyEvent, DummyEvent>
  {

    @Override
    public void bucketLoaded(AbstractBucket<DummyEvent> bucket)
    {
      super.bucketLoaded(bucket);
    }

    @Override
    public DummyEvent convert(DummyEvent dummyEvent)
    {
      return dummyEvent;
    }
  }

  private static DummyDeduper deduper;
  private static String applicationPath;

  @Test
  public void testDedup()
  {
    List<DummyEvent> events = Lists.newArrayList();
    Calendar calendar = Calendar.getInstance();
    long time = calendar.getTimeInMillis();
    long sixHours = 60*60*1000*6;
    for (int i = 0; i < 8; i++) {
      events.add(new DummyEvent(i, time + (sixHours*i)));
    }
    for (int i = 0; i < 8; i++) {
      events.add(new DummyEvent(i, time + (sixHours*i)));
    }

    com.datatorrent.api.Attribute.AttributeMap.DefaultAttributeMap attributes = new com.datatorrent.api.Attribute.AttributeMap.DefaultAttributeMap();
    attributes.put(DAG.APPLICATION_ID, APP_ID);
    attributes.put(DAG.APPLICATION_PATH, applicationPath);

    deduper.setup(new OperatorContextTestHelper.TestIdOperatorContext(OPERATOR_ID, attributes));
    CollectorTestSink<DummyEvent> collectorTestSink = new CollectorTestSink<DummyEvent>();
    TestUtils.setSink(deduper.output, collectorTestSink);

    logger.debug("start round 0");
    deduper.beginWindow(0);
    testRound(events);
    try {
      Thread.sleep(1000);
    } catch (InterruptedException e) {
      e.printStackTrace();
    }
    deduper.handleIdleTime();
    deduper.endWindow();
    System.out.println(collectorTestSink.collectedTuples);
    Assert.assertEquals("output tuples", 8, collectorTestSink.collectedTuples.size());
    collectorTestSink.clear();
    logger.debug("end round 0");

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

  @BeforeClass
  public static void setup()
  {
    applicationPath = OperatorContextTestHelper.getUniqueApplicationPath(APPLICATION_PATH_PREFIX);
    ExpirableHdfsBucketStore<DummyEvent>  bucketStore = new ExpirableHdfsBucketStore<DummyEvent>();
    deduper = new DummyDeduper();
    TimeBasedBucketManagerImpl<DummyEvent> storageManager = new TimeBasedBucketManagerImpl<DummyEvent>();
    storageManager.setBucketSpanInMillis(1000*60*60*6); //6 hours
    storageManager.setDaysSpan(1);
    storageManager.setMillisPreventingBucketEviction(60000);
    storageManager.setBucketStore(bucketStore);
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
