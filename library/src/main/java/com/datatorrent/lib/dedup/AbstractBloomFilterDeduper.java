/**
 * Copyright (c) 2015 DataTorrent, Inc.
 * All rights reserved.
 */
package com.datatorrent.lib.dedup;

import java.util.Iterator;
import java.util.Map;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Maps;

import com.datatorrent.api.Context.OperatorContext;
import com.datatorrent.api.Operator;
import com.datatorrent.lib.bucket.AbstractBucket;
import com.datatorrent.lib.bucket.bloomFilter.BloomFilter;

/**
 * This is the implementation of Deduper that uses Bloom filters for optimization.
 *
 * @displayName Deduper
 * @category Deduplication
 * @tags dedupe
 *
 * @param <INPUT>
 *          type of input tuple
 * @param <OUTPUT>
 *          type of output tuple
 */
public abstract class AbstractBloomFilterDeduper<INPUT, OUTPUT> extends AbstractDeduper<INPUT, OUTPUT> implements
    Operator.CheckpointListener
{
  static int DEF_BLOOM_EXPECTED_TUPLES = 10000;
  static double DEF_BLOOM_FALSE_POS_PROB = 0.01;

  // Bloom filter configurations
  private boolean isUseBloomFilter = true;
  protected transient Map<Long, BloomFilter<Object>> bloomFilters;
  private int expectedNumTuples = DEF_BLOOM_EXPECTED_TUPLES;
  private double falsePositiveProb = DEF_BLOOM_FALSE_POS_PROB;

  @Override
  public void setup(OperatorContext context)
  {
    super.setup(context);
    if (isUseBloomFilter) {
      bloomFilters = Maps.newHashMap();
    }
  }

  /**
   * {@inheritDoc} Additionally uses Bloom filter to identify if the tuple is a unique tuple directly (i.e. without
   * looking into the bucket for the tuple)
   */
  @Override
  protected void processValid(INPUT tuple, AbstractBucket<INPUT> bucket, long bucketKey)
  {
    if (isUseBloomFilter && !waitingEvents.containsKey(bucketKey)) {
      Object tupleKey = getEventKey(tuple);

      if (bloomFilters.containsKey(bucketKey)) {
        if (!bloomFilters.get(bucketKey).contains(tupleKey)) {
          bloomFilters.get(bucketKey).add(tupleKey); // Add tuple key to Bloom filter

          bucketManager.newEvent(bucketKey, tuple);
          processUnique(tuple, bucket);
          return;
        }
      }
    }
    super.processValid(tuple, bucket, bucketKey);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  protected void processUnique(INPUT tuple, AbstractBucket<INPUT> bucket)
  {
    if (isUseBloomFilter && bucket != null) {
      // Add event to bloom filter
      if (!bloomFilters.containsKey(bucket.bucketKey)) {
        bloomFilters.put(bucket.bucketKey, new BloomFilter<Object>(expectedNumTuples, falsePositiveProb));
      }
      bloomFilters.get(bucket.bucketKey).add(getEventKey(tuple));
    }
    super.processUnique(tuple, bucket);
  }

  @Override
  public void bucketLoaded(AbstractBucket<INPUT> loadedBucket)
  {
    if (isUseBloomFilter) {
      // Load bloom filter for this bucket
      Set<Object> keys = loadedBucket.getWrittenEventKeys();
      if (keys != null) {
        if (!bloomFilters.containsKey(loadedBucket.bucketKey)) {
          bloomFilters.put(loadedBucket.bucketKey, new BloomFilter<Object>(expectedNumTuples, falsePositiveProb));
        }
        BloomFilter<Object> bf = bloomFilters.get(loadedBucket.bucketKey);
        for (Object key : keys) {
          bf.add(key);
        }
      }
    }
    super.bucketLoaded(loadedBucket);
  }

  @Override
  public void bucketDeleted(long bucketKey)
  {
    if (isUseBloomFilter && bloomFilters != null && bloomFilters.containsKey(bucketKey)) {
      // Remove bloom filter for this bucket and all previous buckets
      Iterator<Map.Entry<Long, BloomFilter<Object>>> it = bloomFilters.entrySet().iterator();
      while (it.hasNext()) {
        long key = it.next().getKey();
        if (key <= bucketKey) {
          it.remove();
        }
      }
    }
    super.bucketDeleted(bucketKey);
  }

  @Override
  public void checkpointed(long windowId)
  {
    bucketManager.checkpointed(windowId);
  }

  @Override
  public void committed(long windowId)
  {
    bucketManager.committed(windowId);
  }

  /**
   * Sets whether to use bloom filter
   *
   * @param isUseBloomFilter
   */
  public void setUseBloomFilter(boolean isUseBloomFilter)
  {
    this.isUseBloomFilter = isUseBloomFilter;
  }

  /**
   * Sets expected number of tuples to be inserted in the bloom filter to guarantee the false positive probability as
   * configured
   *
   * @param expectedNumTuples
   */
  public void setExpectedNumTuples(int expectedNumTuples)
  {
    this.expectedNumTuples = expectedNumTuples;
  }

  /**
   * Sets the false positive probability for the bloom filter.
   *
   * @param falsePositiveProb
   */
  public void setFalsePositiveProb(double falsePositiveProb)
  {
    this.falsePositiveProb = falsePositiveProb;
  }

  public boolean isUseBloomFilter()
  {
    return isUseBloomFilter;
  }

  public int getExpectedNumTuples()
  {
    return expectedNumTuples;
  }

  public double getFalsePositiveProb()
  {
    return falsePositiveProb;
  }

  private static final Logger LOGGER = LoggerFactory.getLogger(AbstractBloomFilterDeduper.class);
}
