/**
 * Copyright (c) 2015 DataTorrent, Inc.
 * All rights reserved.
 */
package com.datatorrent.lib.dedup;

import java.io.IOException;
import java.util.Calendar;
import java.util.List;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import com.google.common.collect.Lists;

import com.datatorrent.api.Context;
import com.datatorrent.api.DAG;
import com.datatorrent.lib.bucket.AbstractBucket;
import com.datatorrent.lib.bucket.BucketManagerImpl;
import com.datatorrent.lib.bucket.DummyEvent;
import com.datatorrent.lib.bucket.ExpirableHdfsBucketStore;
import com.datatorrent.lib.bucket.HdfsBucketStore;
import com.datatorrent.lib.bucket.NonOperationalBucketStore;
import com.datatorrent.lib.helper.OperatorContextTestHelper;
import com.datatorrent.lib.testbench.CollectorTestSink;
import com.datatorrent.lib.util.TestUtils;

/**
 * Tests for {@link Deduper} optimizations
 */
public class DeduperOptimizationsTest
{
  private static final String APPLICATION_PATH_PREFIX = "target/DeduperOptimizationsTest";
  private static final String APP_ID = "DeduperOptimizationsTest";
  private static final int OPERATOR_ID = 0;
  public static int count = 0;
  public Calendar calendar = Calendar.getInstance();

  private static class DummyDeduper extends AbstractBloomFilterDeduper<DummyEvent, DummyEvent>
  {
    @Override
    public void setup(Context.OperatorContext context)
    {
      boolean stateless = context.getValue(Context.OperatorContext.STATELESS);
      if (stateless) {
        bucketManager.setBucketStore(new NonOperationalBucketStore<DummyEvent>());
      } else {
        ((HdfsBucketStore<DummyEvent>)bucketManager.getBucketStore()).setConfiguration(context.getId(),
            context.getValue(DAG.APPLICATION_PATH), partitionKeys, partitionMask);
      }
      super.setup(context);
    }

    @Override
    public void bucketLoaded(AbstractBucket<DummyEvent> bucket)
    {
      super.bucketLoaded(bucket);
      logger.debug("Bucket Loaded {}", bucket.bucketKey);
    }

    @Override
    public void bucketOffLoaded(long bucketKey)
    {
      super.bucketOffLoaded(bucketKey);
      logger.debug("Bucket Offloaded {}", bucketKey);
    }

    @Override
    public DummyEvent convert(DummyEvent dummyEvent)
    {
      return dummyEvent;
    }

    @Override
    protected Object getEventKey(DummyEvent event)
    {
      return event.getEventKey();
    }
  }

  private static class DummyBucketManagerOptimized extends
      BucketManagerImpl<DummyEvent>
  {
    @Override
    public long getBucketKeyFor(DummyEvent event)
    {
      return Math.abs((int)event.getEventKey()) % noOfBuckets;
    }

    @Override
    public void endWindow(long window)
    {
      super.endWindow(window);
      count++;
    }
  }

  private DummyDeduper deduper;
  private String applicationPath;

  @Test
  public void testOrdering() throws IOException
  {
    List<DummyEvent> events = Lists.newArrayList();
    com.datatorrent.api.Attribute.AttributeMap.DefaultAttributeMap attributes =
        new com.datatorrent.api.Attribute.AttributeMap.DefaultAttributeMap();
    attributes.put(DAG.APPLICATION_ID, APP_ID);
    attributes.put(DAG.APPLICATION_PATH, applicationPath);
    // enable ordered output
    deduper.setOrderedOutput(true);
    deduper.setup(new OperatorContextTestHelper.TestIdOperatorContext(OPERATOR_ID, attributes));
    CollectorTestSink<DummyEvent> unique = new CollectorTestSink<DummyEvent>();
    TestUtils.setSink(deduper.output, unique);
    CollectorTestSink<DummyEvent> duplicate = new CollectorTestSink<DummyEvent>();
    TestUtils.setSink(deduper.duplicates, duplicate);

    //Simple first 5 tuples
    logger.debug("start round 0");
    deduper.beginWindow(0);
    events.clear();
    for (int i = 0; i < 5; i++) {
      events.add(new DummyEvent(i, calendar.getTimeInMillis()));
    }
    testRound(events);
    deduper.handleIdleTime();
    deduper.endWindow();
    Assert.assertEquals("unique tuples", 5, unique.collectedTuples.size());
    for (int i = 0; i < 4; i++) {
      Assert.assertTrue(unique.collectedTuples.get(i).getTime() <= unique.collectedTuples.get(i + 1).getTime());
    }
    unique.clear();
    logger.debug("end round 0");

    /*
     * This should offload the buckets and load new buckets
     * Buckets for first 5 tuples will be available in memory
     * But these would have to wait because of one event which needs to load a bucket
     * Order must be maintained
     */
    events.clear();
    logger.debug("start round 1");
    deduper.beginWindow(1);
    // Needs to load a bucket and waits
    events.add(new DummyEvent(5, calendar.getTimeInMillis()));
    for (int i = 0; i < 5; i++) {
      // These must wait for 5
      events.add(new DummyEvent(i, calendar.getTimeInMillis()));
    }
    testRound(events);
    Assert.assertEquals("duplicate tuples", 0, duplicate.collectedTuples.size());
    Assert.assertEquals("unique tuples", 0, unique.collectedTuples.size());
    deduper.handleIdleTime();
    deduper.endWindow();
    Assert.assertEquals("unique tuples", 1, unique.collectedTuples.size());
    Assert.assertEquals("duplicate tuples", 5, duplicate.collectedTuples.size());
    deduper.teardown();
  }

  private void testRound(List<DummyEvent> events)
  {
    for (DummyEvent event : events) {
      deduper.input.process(event);
    }
  }

  @Before
  public void setup()
  {
    applicationPath = OperatorContextTestHelper.getUniqueApplicationPath(APPLICATION_PATH_PREFIX);
    ExpirableHdfsBucketStore<DummyEvent> bucketStore = new ExpirableHdfsBucketStore<DummyEvent>();
    deduper = new DummyDeduper();
    DummyBucketManagerOptimized storageManager = new DummyBucketManagerOptimized();
    storageManager.setBucketStore(bucketStore);
    storageManager.setNoOfBuckets(10);
    storageManager.setNoOfBucketsInMemory(5);
    storageManager.setMaxNoOfBucketsInMemory(5);
    deduper.setBucketManager(storageManager);
    deduper.setUseBloomFilter(false);
  }

  @After
  public void teardown()
  {
    Path root = new Path(applicationPath);
    try {
      FileSystem fs = FileSystem.newInstance(root.toUri(), new Configuration());
      fs.delete(root, true);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  private static final Logger logger = LoggerFactory.getLogger(DeduperOptimizationsTest.class);
}
