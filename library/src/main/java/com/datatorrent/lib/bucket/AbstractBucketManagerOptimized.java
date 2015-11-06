/**
 * Copyright (c) 2015 DataTorrent, Inc.
 * All rights reserved.
 */
package com.datatorrent.lib.bucket;

import java.util.Map;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

/**
 * This implementation of AbstractBucketManager contains optimizations.
 * Configurable properties: {@link #collateFilesForBucket}: Whether or not to
 * collate all data for a bucket when writing into HDFS
 *
 * Additionally this implementation of AbstractBucketManager will use
 * AutoMetrics as listed below: {@link #deletedBuckets},
 * {@link #bucketsInMemory}, {@link #eventsInMemory}, {@link #evictedBuckets},
 * {@link #eventsCommittedLastWindow}
 *
 * @param <T>
 *          event type
 */
public abstract class AbstractBucketManagerOptimized<T> extends AbstractBucketManager<T>
{

  private boolean collateFilesForBucket = false;
  protected Set<Integer> bucketsToDelete;

  public AbstractBucketManagerOptimized()
  {
    super();
    bucketsToDelete = Sets.newHashSet();
  }

  @Override
  public void startService(com.datatorrent.lib.bucket.BucketManager.Listener<T> listener)
  {
    super.startService(listener);
    recordStats = true; // Always record stats
  }

  /**
   * {@inheritDoc}
   */
  @Override
  protected Map<Integer, Map<Object, T>> getDataToStore()
  {
    Map<Integer, Map<Object, T>> dataToStore = Maps.newHashMap();
    for (Map.Entry<Integer, AbstractBucket<T>> entry : dirtyBuckets.entrySet()) {
      AbstractBucket<T> bucket = entry.getValue();
      dataToStore.put(entry.getKey(), bucket.getUnwrittenEvents());

      if (collateFilesForBucket) {
        // Then collate all data together and write into a new file together
        if (bucket.getWrittenEventKeys() != null && !bucket.getWrittenEventKeys().isEmpty()) {
          dataToStore.put(entry.getKey(), bucket.getWrittenEvents());
          bucketsToDelete.add(entry.getKey()); // Record bucket to be deleted
        }
      }

      bucket.transferDataFromMemoryToStore();
      evictionCandidates.add(entry.getKey());
    }
    return dataToStore;
  }

  @Override
  protected void storeData(long window, long id, Map<Integer, Map<Object, T>> dataToStore)
  {
    super.storeData(window, id, dataToStore);
    if (collateFilesForBucket) {
      for (int key : bucketsToDelete) {
        try {
          bucketStore.deleteBucket(key); // Delete recorded buckets if store was successful
        } catch (Exception e) {
          throw new RuntimeException("Error deleting bucket index " + key, e);
        }
      }
    }
  }

  public boolean isCollateFilesForBucket()
  {
    return collateFilesForBucket;
  }

  /**
   * Sets whether entire bucket data must be persisted together in a new file.
   * When this is not set, only the unwritten part of the bucket is persisted.
   *
   * @param collateFilesForBucket
   */
  public void setCollateFilesForBucket(boolean collateFilesForBucket)
  {
    this.collateFilesForBucket = collateFilesForBucket;
  }

  /**
   * An {@link AbstractBucketManagerOptimized} implementation
   *
   * @param <T>
   */
  public static class BucketManagerOptimizedImpl<T extends Bucketable> extends AbstractBucketManagerOptimized<T>
  {
    @Override
    protected Bucket<T> createBucket(long bucketKey)
    {
      return new Bucket<T>(bucketKey);
    }

    @Override
    public long getBucketKeyFor(T event)
    {
      return Math.abs(event.getEventKey().hashCode()) % noOfBuckets;
    }

    /*
     * This method has been deprecated. Use clone instead.
     */
    @Deprecated
    @Override
    public BucketManagerOptimizedImpl<T> cloneWithProperties()
    {
      throw new UnsupportedOperationException();
    }

  }

  private static final transient Logger LOGGER = LoggerFactory.getLogger(AbstractBucketManagerOptimized.class);

}
