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

import com.datatorrent.lib.bucket.SimpleEvent;
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

public class DeduperSimpleTest
{
  private static final Logger logger = LoggerFactory.getLogger(DeduperSimpleTest.class);

  private final static String APPLICATION_PATH_PREFIX = "target/DeduperAppBuilderTest";
  private final static String APP_ID = "DeduperAppBuilderTest";
  private final static int OPERATOR_ID = 0;
  private static String timestamp;

  private final static Exchanger<Long> eventBucketExchanger = new Exchanger<Long>();

  private static class DummyDeduper extends DeduperPOJOImpl<SimpleEvent,SimpleEvent>
  {

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
    SimpleEvent temp = new SimpleEvent();

    for (int i = 0; i < 10; i++) {
    temp.setId(i);
    temp.setHhmm(calendar.getTimeInMillis());
    }
    temp.setId(5);
    temp.setHhmm(calendar.getTimeInMillis());

    events.add(temp);

    com.datatorrent.api.Attribute.AttributeMap.DefaultAttributeMap attributes = new com.datatorrent.api.Attribute.AttributeMap.DefaultAttributeMap();
    attributes.put(DAG.APPLICATION_ID, APP_ID);
    attributes.put(DAG.APPLICATION_PATH, applicationPath);

    deduper.setup(new OperatorContextTestHelper.TestIdOperatorContext(OPERATOR_ID, attributes));
    CollectorTestSink<HashMap<String,Object>> collectorTestSink = new CollectorTestSink<HashMap<String,Object>>();
    TestUtils.setSink(deduper.output, collectorTestSink);

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
   // Assert.assertEquals("output tuples", 0, collectorTestSink.collectedTuples.size());
    collectorTestSink.clear();
    logger.debug("end round 1");

    deduper.teardown();
  }


   private void testRound(List<SimpleEvent> events)
  {
    for (SimpleEvent event : events) {
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
    ExpirableHdfsBucketStore<HashMap<String,Object>>  bucketStore = new ExpirableHdfsBucketStore<HashMap<String,Object>>();
    deduper = new DummyDeduper();

    TimeBasedBucketManagerPOJOImpl timeManager = new TimeBasedBucketManagerPOJOImpl();
    timeManager.setBucketSpanInMillis(60000);
    timeManager.setMillisPreventingBucketEviction(60000);
    timeManager.setBucketStore(bucketStore);
    //storageManager.setCustomKey(customKey);
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