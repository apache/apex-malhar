/**
 * Copyright (c) 2015 DataTorrent, Inc.
 * All rights reserved.
 */
package com.datatorrent.lib.dimensions;

import it.unimi.dsi.fastutil.ints.Int2ObjectMap;
import it.unimi.dsi.fastutil.ints.Int2ObjectOpenHashMap;
import it.unimi.dsi.fastutil.objects.Object2IntMap;
import it.unimi.dsi.fastutil.objects.Object2IntOpenHashMap;
import java.io.Serializable;

import java.util.HashMap;
import java.util.Map;


import com.google.common.base.Preconditions;

import com.datatorrent.lib.appdata.schemas.CustomTimeBucket;

public class CustomTimeBucketRegistry implements Serializable
{
  private static final long serialVersionUID = 201509221536L;

  private int currentId;

  private Int2ObjectMap<CustomTimeBucket> idToTimeBucket = new Int2ObjectOpenHashMap<>();
  private Object2IntMap<CustomTimeBucket> timeBucketToId = new Object2IntOpenHashMap<>();
  private Map<String, CustomTimeBucket> textToTimeBucket = new HashMap<>();

  public CustomTimeBucketRegistry()
  {
  }

  public CustomTimeBucketRegistry(int startingId)
  {
    this.currentId = startingId;
  }

  public CustomTimeBucketRegistry(Int2ObjectMap<CustomTimeBucket> idToTimeBucket)
  {
    initialize(idToTimeBucket);
  }

  public CustomTimeBucketRegistry(Int2ObjectMap<CustomTimeBucket> idToTimeBucket,
                                  int startingId)
  {
    int tempId = initialize(idToTimeBucket);

    Preconditions.checkArgument(tempId < startingId, "The statingId " + startingId
                                                     + " must be larger than the largest ID " + tempId
                                                     + " in the given idToTimeBucket mapping");

    this.idToTimeBucket = Preconditions.checkNotNull(idToTimeBucket);
    this.currentId = startingId;
  }

  private int initialize(Int2ObjectMap<CustomTimeBucket> idToTimeBucket)
  {
    Preconditions.checkNotNull(idToTimeBucket);

    int tempId = Integer.MIN_VALUE;

    for (int timeBucketId : idToTimeBucket.keySet()) {
      tempId = Math.max(tempId, timeBucketId);
      CustomTimeBucket customTimeBucket = idToTimeBucket.get(timeBucketId);
      textToTimeBucket.put(customTimeBucket.getText(), customTimeBucket);
      Preconditions.checkNotNull(customTimeBucket);
      timeBucketToId.put(customTimeBucket, timeBucketId);
    }

    return tempId;
  }

  public CustomTimeBucket getTimeBucket(int timeBucketId)
  {
    return idToTimeBucket.get(timeBucketId);
  }

  public Integer getTimeBucketId(CustomTimeBucket timeBucket)
  {
    if (!timeBucketToId.containsKey(timeBucket)) {
      return null;
    }

    return timeBucketToId.get(timeBucket);
  }

  public CustomTimeBucket getTimeBucket(String text)
  {
    return textToTimeBucket.get(text);
  }

  public void register(CustomTimeBucket timeBucket)
  {
    register(timeBucket, currentId);
  }

  public void register(CustomTimeBucket timeBucket, int timeBucketId)
  {
    if (timeBucketToId.containsKey(timeBucket)) {
      throw new IllegalArgumentException("The timeBucket " + timeBucket + " is already registered.");
    }

    if (timeBucketToId.containsValue(timeBucketId)) {
      throw new IllegalArgumentException("The timeBucketId " + timeBucketId + " is already registered.");
    }

    idToTimeBucket.put(timeBucketId, timeBucket);
    timeBucketToId.put(timeBucket, timeBucketId);

    if (timeBucketId >= currentId) {
      currentId = timeBucketId + 1;
    }

    textToTimeBucket.put(timeBucket.getText(), timeBucket);
  }

  @Override
  public String toString()
  {
    return "CustomTimeBucketRegistry{" + "idToTimeBucket=" + idToTimeBucket + '}';
  }

}
