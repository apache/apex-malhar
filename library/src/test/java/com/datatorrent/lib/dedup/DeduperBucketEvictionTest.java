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
import com.datatorrent.lib.bucket.BucketManagerImpl;
import com.datatorrent.lib.bucket.DummyEvent;
import com.datatorrent.lib.bucket.ExpirableHdfsBucketStore;
import com.datatorrent.lib.bucket.TimeBasedBucketManagerImpl;
import com.datatorrent.lib.helper.OperatorContextTestHelper;
import com.datatorrent.lib.testbench.CollectorTestSink;
import com.datatorrent.lib.util.TestUtils;

/**
 * Tests for {@link Deduper}
 */
public class DeduperBucketEvictionTest
{
  private static final Logger logger = LoggerFactory.getLogger(DeduperBucketEvictionTest.class);

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
      try {
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
  }

  private static DummyDeduper deduper;
  private static BucketManagerImpl<DummyEvent> manager;
  private static String applicationPath;

  @Test
  public void testBucketReferenceRemovalOnEviction() throws InterruptedException
  {
    com.datatorrent.api.Attribute.AttributeMap.DefaultAttributeMap attributes = new com.datatorrent.api.Attribute.AttributeMap.DefaultAttributeMap();
    attributes.put(DAG.APPLICATION_ID, APP_ID);
    attributes.put(DAG.APPLICATION_PATH, applicationPath);

    deduper.setup(new OperatorContextTestHelper.TestIdOperatorContext(OPERATOR_ID, attributes));

    deduper.beginWindow(1);
    manager.loadBucketData(2);
    eventBucketExchanger.exchange(null);
    manager.newEvent(2, new DummyEvent(1, 100));
    Assert.assertEquals(manager.getBucket(2).countOfUnwrittenEvents(), 1);
    deduper.endWindow();

    deduper.beginWindow(2);
    manager.loadBucketData(2883);
    eventBucketExchanger.exchange(null);
    deduper.endWindow();

    deduper.beginWindow(3);
    manager.loadBucketData(2882);
    eventBucketExchanger.exchange(null);
    manager.loadBucketData(-2);
    deduper.endWindow();

    Assert.assertEquals(manager.getBucket(2882).countOfWrittenEvents(), 0);
  }

  @BeforeClass
  public static void setup()
  {
    applicationPath = OperatorContextTestHelper.getUniqueApplicationPath(APPLICATION_PATH_PREFIX);
    ExpirableHdfsBucketStore<DummyEvent>  bucketStore = new ExpirableHdfsBucketStore<DummyEvent>();
    deduper = new DummyDeduper();
    manager = new BucketManagerImpl<DummyEvent>();
    manager.setNoOfBuckets(2880);
    manager.setNoOfBucketsInMemory(1);
    manager.setMaxNoOfBucketsInMemory(1);
    manager.setMillisPreventingBucketEviction(1);
    manager.setBucketStore(bucketStore);
    deduper.setBucketManager(manager);
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
