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

import com.datatorrent.lib.bucket.*;
import com.datatorrent.lib.helper.OperatorContextTestHelper;
import com.datatorrent.lib.testbench.CollectorTestSink;
import com.datatorrent.lib.util.TestUtils;
import java.util.*;

public class DeduperAppBuilderTest
{
  private static final Logger logger = LoggerFactory.getLogger(DeduperAppBuilderTest.class);

  private final static String APPLICATION_PATH_PREFIX = "target/DeduperAppBuilderTest";
  private final static String APP_ID = "DeduperAppBuilderTest";
  private final static int OPERATOR_ID = 0;
  private static String timestamp;

  private final static Exchanger<Long> eventBucketExchanger = new Exchanger<Long>();

  private static class DummyDeduper extends DeduperWithAppBuilder
  {

    @Override
    public void bucketLoaded(Bucket<HashMap<String,Object>> bucket)
    {
      try {
        super.bucketLoaded(bucket);
        eventBucketExchanger.exchange(bucket.bucketKey);
      }
      catch (InterruptedException e) {
        throw new RuntimeException(e);
      }
    }


    public void addEventManuallyToWaiting(HashMap<String,Object> event)
    {
      waitingEvents.put(bucketManager.getBucketKeyFor(event), Lists.newArrayList(event));
    }

  }

  private static DummyDeduper deduper;
  private static String applicationPath;

  @Test
  public void testDedup()
  {
    List<HashMap<String,Object>> events = Lists.newArrayList();
    Calendar calendar = Calendar.getInstance();

    for (int i = 0; i < 10; i++) {
      long now = System.currentTimeMillis();
      HashMap<String,Object> temp = new HashMap<String, Object>();
      temp.put(i+"",calendar.getTimeInMillis() );
      temp.put(timestamp, calendar.getTimeInMillis());
      events.add(temp);
    }
    HashMap<String,Object> temp = new HashMap<String, Object>();
    temp.put(5+"", calendar.getTimeInMillis());
    temp.put(timestamp, calendar.getTimeInMillis());

    events.add(temp);

    com.datatorrent.api.Attribute.AttributeMap.DefaultAttributeMap attributes = new com.datatorrent.api.Attribute.AttributeMap.DefaultAttributeMap();
    attributes.put(DAG.APPLICATION_ID, APP_ID);
    attributes.put(DAG.APPLICATION_PATH, applicationPath);

    deduper.setup(new OperatorContextTestHelper.TestIdOperatorContext(OPERATOR_ID, attributes));
    CollectorTestSink<DummyEvent> collectorTestSink = new CollectorTestSink<DummyEvent>();
    TestUtils.setSink(deduper.output, collectorTestSink);

    logger.debug("start round 0");
    deduper.beginWindow(0);
    testRound(events);
 //   deduper.handleIdleTime();
    deduper.endWindow();
    Assert.assertEquals("output tuples", 10, collectorTestSink.collectedTuples.size());
    collectorTestSink.clear();
    logger.debug("end round 0");

    logger.debug("start round 1");
    deduper.beginWindow(1);
    testRound(events);
    deduper.handleIdleTime();
    deduper.endWindow();
   // Assert.assertEquals("output tuples", 0, collectorTestSink.collectedTuples.size());
    collectorTestSink.clear();
    logger.debug("end round 1");

    /*Test the sliding window
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
    logger.debug("end round 2");*/
    deduper.teardown();
  }

  private void testRound(List<HashMap<String,Object>> events)
  {
    for (HashMap<String,Object> event : events) {
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
    com.datatorrent.api.Attribute.AttributeMap.DefaultAttributeMap attributes = new com.datatorrent.api.Attribute.AttributeMap.DefaultAttributeMap();
    attributes.put(DAG.APPLICATION_ID, APP_ID);
    attributes.put(DAG.APPLICATION_PATH, applicationPath);
    HashMap<String,Object> temp = new HashMap<String, Object>();
    temp.put(100+"", System.currentTimeMillis());
    deduper.addEventManuallyToWaiting(temp);
    deduper.setup(new OperatorContextTestHelper.TestIdOperatorContext(0, attributes));
    eventBucketExchanger.exchange(null, 500, TimeUnit.MILLISECONDS);
    deduper.endWindow();
    deduper.teardown();
  }

  @BeforeClass
  public static void setup()
  {
    Calendar calendar = Calendar.getInstance();
    applicationPath = OperatorContextTestHelper.getUniqueApplicationPath(APPLICATION_PATH_PREFIX);
    ExpirableHdfsBucketStore<HashMap<String,Object>>  bucketStore = new ExpirableHdfsBucketStore<HashMap<String,Object>>();
    deduper = new DummyDeduper();
    BucketableCustomKey customKey = new BucketableCustomKey();
    ArrayList<Object> input = new ArrayList<Object>();
    for (int i = 0; i < 10; i++) {
      input.add(i + "");
    }
    customKey.setKey(input);
    timestamp = "20150325";
    customKey.setTime(timestamp);
    TimeBasedBucketManagerImpl storageManager = new TimeBasedBucketManagerImpl();
    storageManager.setBucketSpanInMillis(60000);
    storageManager.setMillisPreventingBucketEviction(60000);
    storageManager.setBucketStore(bucketStore);
    storageManager.setCustomKey(customKey);
    deduper.setBucketManager(storageManager);

    deduper.setCustomKey(customKey);

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