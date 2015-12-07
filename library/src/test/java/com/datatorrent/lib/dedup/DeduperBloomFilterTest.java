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
 * Tests for {@link Deduper} using Bloom Filter
 */
public class DeduperBloomFilterTest
{
  private static final Logger logger = LoggerFactory.getLogger(DeduperBloomFilterTest.class);

  private static final String APPLICATION_PATH_PREFIX = "target/DeduperBloomFilterTest";
  private static final String APP_ID = "DeduperBloomFilterTest";
  private static final int OPERATOR_ID = 0;

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
  }

  private DummyDeduper deduper;
  private String applicationPath;

  @Test
  public void testDedup() throws IOException
  {
    List<DummyEvent> events = Lists.newArrayList();
    Calendar calendar = Calendar.getInstance();
    for (int i = 0; i < 10; i++) {
      events.add(new DummyEvent(i, calendar.getTimeInMillis()));
    }
    events.add(new DummyEvent(5, calendar.getTimeInMillis()));

    com.datatorrent.api.Attribute.AttributeMap.DefaultAttributeMap attributes =
        new com.datatorrent.api.Attribute.AttributeMap.DefaultAttributeMap();
    attributes.put(DAG.APPLICATION_ID, APP_ID);
    attributes.put(DAG.APPLICATION_PATH, applicationPath);

    deduper.setup(new OperatorContextTestHelper.TestIdOperatorContext(OPERATOR_ID, attributes));
    CollectorTestSink<DummyEvent> collectorTestSink = new CollectorTestSink<DummyEvent>();
    TestUtils.setSink(deduper.output, collectorTestSink);

    logger.debug("start round 0");
    deduper.beginWindow(0);
    testRound(events);
    deduper.handleIdleTime();
    deduper.endWindow();
    Assert.assertEquals("output tuples", 10, collectorTestSink.collectedTuples.size());
    collectorTestSink.clear();
    logger.debug("end round 0");

    // Uniques with existing buckets. Will use bloom filter
    for (int i = 10; i < 15; i++) {
      events.add(new DummyEvent(i, calendar.getTimeInMillis()));
    }
    logger.debug("start round 1");
    deduper.beginWindow(1);
    testRound(events);
    deduper.handleIdleTime();
    deduper.endWindow();
    Assert.assertEquals("output tuples", 5, collectorTestSink.collectedTuples.size());
    collectorTestSink.clear();
    logger.debug("end round 1");

    logger.debug("start round 2");
    deduper.beginWindow(2);
    testRound(events);
    deduper.handleIdleTime();
    deduper.endWindow();
    Assert.assertEquals("output tuples", 0, collectorTestSink.collectedTuples.size());
    collectorTestSink.clear();
    logger.debug("end round 2");

    deduper.teardown();
  }

  @Test
  public void testBloomFilterLoading() throws IOException
  {
    List<DummyEvent> events = Lists.newArrayList();
    Calendar calendar = Calendar.getInstance();
    com.datatorrent.api.Attribute.AttributeMap.DefaultAttributeMap attributes =
        new com.datatorrent.api.Attribute.AttributeMap.DefaultAttributeMap();
    attributes.put(DAG.APPLICATION_ID, APP_ID);
    attributes.put(DAG.APPLICATION_PATH, applicationPath);

    deduper.setup(new OperatorContextTestHelper.TestIdOperatorContext(OPERATOR_ID, attributes));
    CollectorTestSink<DummyEvent> collectorTestSink = new CollectorTestSink<DummyEvent>();
    TestUtils.setSink(deduper.output, collectorTestSink);

    //Bucket offloading, bloom filter deletion test
    logger.debug("start round 0");
    deduper.beginWindow(0);
    events.clear();
    for (int i = 0; i < 5; i++) {
      events.add(new DummyEvent(i, calendar.getTimeInMillis()));
    }
    testRound(events);
    deduper.handleIdleTime();
    deduper.endWindow();
    Assert.assertEquals("output tuples", 5, collectorTestSink.collectedTuples.size());
    collectorTestSink.clear();
    logger.debug("end round 0");

    logger.debug("start round 1");
    deduper.beginWindow(1);
    events.clear();
    for (int i = 5; i < 10; i++) {
      events.add(new DummyEvent(i, calendar.getTimeInMillis()));
    }
    testRound(events);
    deduper.handleIdleTime();
    deduper.endWindow();
    Assert.assertEquals("output tuples", 5, collectorTestSink.collectedTuples.size());
    collectorTestSink.clear();
    logger.debug("end round 1");

    /*
     * Buckets 0-4 offloaded now
     * Try loading the offloaded buckets and check of uniques are detected
     */

    logger.debug("start round 2");
    deduper.beginWindow(2);
    events.clear();
    for (int i = 0; i < 15; i++) {
      events.add(new DummyEvent(i, calendar.getTimeInMillis()));
    }
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
    deduper.setUseBloomFilter(true);
    deduper.setExpectedNumTuples(1000);
    deduper.setFalsePositiveProb(0.01);
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
}
