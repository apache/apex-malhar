/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

package com.datatorrent.ada.hdht;

import com.datatorrent.ada.counters.DataGroup;
import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import it.unimi.dsi.fastutil.objects.Object2LongOpenHashMap;

import java.util.Map;
import java.util.Set;

/**
 *
 * @author Timothy Farkas: tim@datatorrent.com
 */
public class BucketTracker
{
  public static final long DEFUALT_RETURN_VALUE = 0L;
  public static final long FIRST_ID = DEFUALT_RETURN_VALUE + 1;

  //TODO Store these maps in HDHT
  private Object2LongOpenHashMap<BucketIdentifier> bucketIdentifierToBucketId = new Object2LongOpenHashMap<BucketIdentifier>();
  private Map<DataGroup, Set<BucketIdentifier>> dataGroupToBuckets = Maps.newHashMap();
  private long nextId = 1L;

  public BucketTracker()
  {
    bucketIdentifierToBucketId.defaultReturnValue(0L);
  }

  public synchronized long getBucketId(BucketIdentifier bucketIdentifier)
  {
    Preconditions.checkNotNull(bucketIdentifier);

    long bucketId = bucketIdentifierToBucketId.get(bucketIdentifier);

    if(bucketId == DEFUALT_RETURN_VALUE) {
      bucketId = nextId;
      bucketIdentifierToBucketId.put(bucketIdentifier, nextId);
      nextId++;
    }

    Set<BucketIdentifier> bucketIds = dataGroupToBuckets.get(bucketIdentifier);

    if(bucketIds == null) {
      bucketIds = Sets.newHashSet();
      dataGroupToBuckets.put(bucketIdentifier.getDatagroup(), bucketIds);
    }

    bucketIds.add(bucketIdentifier);

    return bucketId;
  }

  public synchronized Set<BucketIdentifier> getBucketIds(DataGroup dataGroup)
  {
    return dataGroupToBuckets.get(dataGroup);
  }
}
