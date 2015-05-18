/*
 * Copyright (c) 2015 DataTorrent, Inc. ALL Rights Reserved.
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

import com.datatorrent.lib.bucket.TimeBasedBucketManagerPOJOImpl;
import com.datatorrent.api.Context;
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

public class DeduperSimpleEventTest
{
  private static final Logger logger = LoggerFactory.getLogger(DeduperSimpleEventTest.class);

  private final static String APPLICATION_PATH_PREFIX = "target/DeduperPOJOTest";
  private final static String APP_ID = "DeduperPOJOTest";
  private final static int OPERATOR_ID = 0;
  private static TimeBasedBucketManagerSimpleEventImpl timeManager;
  private final static Exchanger<Long> eventBucketExchanger = new Exchanger<Long>();

  private static class DummyDeduper extends AbstractDeduper<SimpleEvent, SimpleEvent>
  {
    @Override
    public void setup(Context.OperatorContext context)
    {
      boolean stateless = context.getValue(Context.OperatorContext.STATELESS);
      if (stateless) {
        bucketManager.setBucketStore(new NonOperationalBucketStore<SimpleEvent>());
      }
      else {
        ((HdfsBucketStore<SimpleEvent>)bucketManager.getBucketStore()).setConfiguration(context.getId(), context.getValue(DAG.APPLICATION_PATH), partitionKeys, partitionMask);
      }
      super.setup(context);
    }

    @Override
    public void bucketLoaded(AbstractBucket<SimpleEvent> bucket)
    {
      try {
        super.bucketLoaded(bucket);
        eventBucketExchanger.exchange(bucket.bucketKey);
      }
      catch (InterruptedException e) {
        throw new RuntimeException(e);
      }
    }

    public void addEventManuallyToWaiting(SimpleEvent event)
    {
      waitingEvents.put(bucketManager.getBucketKeyFor(event), Lists.newArrayList(event));
    }

    @Override
    protected SimpleEvent convert(SimpleEvent input)
    {
      return input;
    }

    @Override
    protected Object getEventKey(SimpleEvent event)
    {
      return event.getId();
    }

  }

  private static DummyDeduper deduper;
  private static String applicationPath;

  @Test
  public void testDedup()
  {
    List<SimpleEvent> events = Lists.newArrayList();
    Calendar calendar = Calendar.getInstance();
    for (int i = 0; i < 10; i++) {
      SimpleEvent event = new SimpleEvent();
      event.setId(i);
      event.setHhmm(calendar.getTimeInMillis() + "");
      events.add(event);
    }
    //Add a duplicate event.
    SimpleEvent event = new SimpleEvent();
    event.setId(5);
    event.setHhmm(calendar.getTimeInMillis() + "");

    events.add(event);

    //Add an expired event.
    Calendar newYearsDay = Calendar.getInstance();
    newYearsDay.set(2013, 0, 1, 0, 0, 0);
    event = new SimpleEvent();
    event.setId(5);
    event.setHhmm(newYearsDay.getTimeInMillis()+"");
    events.add(event);

    com.datatorrent.api.Attribute.AttributeMap.DefaultAttributeMap attributes = new com.datatorrent.api.Attribute.AttributeMap.DefaultAttributeMap();
    attributes.put(DAG.APPLICATION_ID, APP_ID);
    attributes.put(DAG.APPLICATION_PATH, applicationPath);

    deduper.setup(new OperatorContextTestHelper.TestIdOperatorContext(OPERATOR_ID, attributes));
    CollectorTestSink<SimpleEvent> collectorTestSink = new CollectorTestSink<SimpleEvent>();
    CollectorTestSink<SimpleEvent> collectorTestSinkDuplicates = new CollectorTestSink<SimpleEvent>();
    CollectorTestSink<SimpleEvent> collectorTestSinkIgnored = new CollectorTestSink<SimpleEvent>();

    TestUtils.setSink(deduper.output, collectorTestSink);
    TestUtils.setSink(deduper.duplicates, collectorTestSinkDuplicates);
    TestUtils.setSink(timeManager.ignored, collectorTestSinkIgnored);

    logger.debug("start round 0");
    deduper.beginWindow(0);
    testRound(events);
    deduper.handleIdleTime();
    deduper.endWindow();
    Assert.assertEquals("output tuples", 10, collectorTestSink.collectedTuples.size());
    Assert.assertEquals("deduper duplicates", 1,collectorTestSinkDuplicates.collectedTuples.size());
    Assert.assertEquals("ignored events", 1,collectorTestSinkIgnored.collectedTuples.size());

    collectorTestSink.clear();
    collectorTestSinkDuplicates.clear();
    collectorTestSinkIgnored.clear();
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
      event = new SimpleEvent();
      event.setId(i);
      event.setHhmm(now + "");
      events.add(event);
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

  @Test
  public void testDeduperRedeploy() throws Exception
  {
    com.datatorrent.api.Attribute.AttributeMap.DefaultAttributeMap attributes = new com.datatorrent.api.Attribute.AttributeMap.DefaultAttributeMap();
    attributes.put(DAG.APPLICATION_ID, APP_ID);
    attributes.put(DAG.APPLICATION_PATH, applicationPath);
    SimpleEvent simpleEvent = new SimpleEvent();
    simpleEvent.setId(100);
    simpleEvent.setHhmm(System.currentTimeMillis() + "");
    deduper.addEventManuallyToWaiting(simpleEvent);
    deduper.setup(new OperatorContextTestHelper.TestIdOperatorContext(0, attributes));
    eventBucketExchanger.exchange(null, 500, TimeUnit.MILLISECONDS);
    deduper.endWindow();
    deduper.teardown();
  }

  private void testRound(List<SimpleEvent> events)
  {
    for (SimpleEvent event: events) {
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
    ExpirableHdfsBucketStore<SimpleEvent> bucketStore = new ExpirableHdfsBucketStore<SimpleEvent>();
    deduper = new DummyDeduper();
    timeManager = new TimeBasedBucketManagerSimpleEventImpl();
    timeManager.setBucketSpanInMillis(60000);
    timeManager.setMillisPreventingBucketEviction(60000);
    timeManager.setBucketStore(bucketStore);
    deduper.setBucketManager(timeManager);
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
