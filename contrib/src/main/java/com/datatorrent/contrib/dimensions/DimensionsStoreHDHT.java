/*
 * Copyright (c) 2015 DataTorrent, Inc. ALL Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.datatorrent.contrib.dimensions;

import com.datatorrent.api.annotation.OperatorAnnotation;
import com.datatorrent.common.util.Slice;
import com.datatorrent.contrib.hdht.AbstractSinglePortHDHTWriter;
import com.datatorrent.lib.appdata.dimensions.AggregateEvent;
import com.datatorrent.lib.appdata.dimensions.AggregateEvent.EventKey;
import com.datatorrent.lib.appdata.dimensions.DimensionsDescriptor;
import com.datatorrent.lib.appdata.dimensions.DimensionsStaticAggregator;
import com.datatorrent.lib.appdata.gpo.GPOByteArrayList;
import com.datatorrent.lib.appdata.gpo.GPOMutable;
import com.datatorrent.lib.appdata.gpo.GPOUtils;
import com.datatorrent.lib.appdata.schemas.FieldsDescriptor;
import com.datatorrent.lib.codec.KryoSerializableStreamCodec;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import com.google.common.primitives.Ints;
import com.google.common.primitives.Longs;
import java.io.IOException;
import javax.validation.constraints.Min;
import org.apache.commons.lang3.mutable.MutableInt;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;

/**
 * TODO aggregate by windowID in waiting cache.
 */
@OperatorAnnotation(checkpointableWithinAppWindow=false)
public abstract class DimensionsStoreHDHT extends AbstractSinglePortHDHTWriter<AggregateEvent>
{
  public static final int CACHE_SIZE = 100000;
  public static final int DEFAULT_CACHE_WINDOW_DURATION = 120;

  public static final int META_DATA_ID_WINDOW_ID = 0;
  public static final Slice WINDOW_ID_KEY = new Slice(GPOUtils.serializeInt(META_DATA_ID_WINDOW_ID));
  public static final int META_DATA_ID_STORE_FORMAT = 1;
  public static final Slice STORE_FORMAT_KEY = new Slice(GPOUtils.serializeInt(META_DATA_ID_STORE_FORMAT));
  public static final int STORE_FORMAT_VERSION = 0;

  @Min(1)
  private int cacheWindowDuration = DEFAULT_CACHE_WINDOW_DURATION;
  private int cacheWindowCount = 0;
  @VisibleForTesting
  protected transient long currentWindowID;

  protected transient Map<EventKey, AggregateEvent> cache = Maps.newHashMap();
  protected List<Long> buckets;

  @VisibleForTesting
  protected transient boolean readMetaData = false;
  @VisibleForTesting
  protected transient final Map<Long, Long> futureBuckets = Maps.newHashMap();

  public DimensionsStoreHDHT()
  {
  }

  protected abstract int getAggregatorID(String aggregatorName);
  protected abstract DimensionsStaticAggregator getAggregator(int aggregatorID);
  protected abstract FieldsDescriptor getKeyDescriptor(int schemaID, int dimensionsDescriptorID);
  protected abstract FieldsDescriptor getValueDescriptor(int schemaID, int dimensionsDescriptorID, int aggregatorID);
  protected abstract long getBucketForSchema(int schemaID);

  protected long getBucketForSchema(EventKey eventKey)
  {
    return getBucketForSchema(eventKey.getSchemaID());
  }

  protected byte[] getKeyBytesGAE(AggregateEvent gae)
  {
    return getEventKeyBytesGAE(gae.getEventKey());
  }

  public byte[] getEventKeyBytesGAE(EventKey eventKey)
  {
    GPOByteArrayList bal = new GPOByteArrayList();

    long timestamp = 0;

    if(eventKey.getKey().
            getFieldDescriptor().getFieldList().
            contains(DimensionsDescriptor.DIMENSION_TIME)) {
      timestamp = eventKey.getKey().getFieldLong(DimensionsDescriptor.DIMENSION_TIME);
    }

    byte[] timeBytes = Longs.toByteArray(timestamp);
    byte[] schemaIDBytes = Ints.toByteArray(eventKey.getSchemaID());
    byte[] dimensionDescriptorIDBytes = Ints.toByteArray(eventKey.getDimensionDescriptorID());
    byte[] aggregatorIDBytes = Ints.toByteArray(eventKey.getAggregatorID());
    byte[] gpoBytes = GPOUtils.serialize(eventKey.getKey(), DimensionsDescriptor.TIME_FIELDS);

    bal.add(timeBytes);
    bal.add(schemaIDBytes);
    bal.add(dimensionDescriptorIDBytes);
    bal.add(aggregatorIDBytes);
    bal.add(gpoBytes);

    return bal.toByteArray();
  }

  public byte[] getValueBytesGAE(AggregateEvent event)
  {
    return GPOUtils.serialize(event.getAggregates());
  }

  public AggregateEvent fromKeyValueGAE(Slice key, byte[] aggregate)
  {
    MutableInt offset = new MutableInt(0);
    long timestamp = GPOUtils.deserializeLong(key.buffer, offset);
    int schemaID = GPOUtils.deserializeInt(key.buffer,
                                           offset);
    int dimensionDescriptorID = GPOUtils.deserializeInt(key.buffer,
                                                        offset);
    int aggregatorID = GPOUtils.deserializeInt(key.buffer,
                                               offset);

    FieldsDescriptor keysDescriptor = getKeyDescriptor(schemaID, dimensionDescriptorID);
    FieldsDescriptor aggDescriptor = getValueDescriptor(schemaID, dimensionDescriptorID, aggregatorID);

    GPOMutable keys = GPOUtils.deserialize(keysDescriptor, DimensionsDescriptor.TIME_FIELDS, key.buffer, offset.intValue());
    GPOMutable aggs = GPOUtils.deserialize(aggDescriptor, aggregate, 0);

    if(keysDescriptor.getFields().getFields().contains(DimensionsDescriptor.DIMENSION_TIME)) {
      keys.setField(DimensionsDescriptor.DIMENSION_TIME, timestamp);
    }

    AggregateEvent gae = new AggregateEvent(keys,
                                            aggs,
                                            schemaID,
                                            dimensionDescriptorID,
                                            aggregatorID);
    return gae;
  }

  public AggregateEvent load(EventKey eventKey)
  {
    long bucket = getBucketForSchema(eventKey);
    byte[] key = getEventKeyBytesGAE(eventKey);

    Slice keySlice = new Slice(key, 0, key.length);
    byte[] val = load(bucket, keySlice);

    if(val == null) {
      return null;
    }

    return fromKeyValueGAE(keySlice, val);
  }

  public byte[] load(long bucketID, Slice keySlice)
  {
    byte[] val = getUncommitted(bucketID, keySlice);

    if(val == null) {
      try {
        val = get(bucketID, keySlice);
      }
      catch(IOException ex) {
        throw new RuntimeException(ex);
      }
    }

    return val;
  }

  public int getPartitionGAE(AggregateEvent inputEvent) {
    return inputEvent.getBucketID();
  }

  public void putGAE(AggregateEvent gae)
  {
    try {
      put(getBucketForSchema(gae.getSchemaID()),
          new Slice(codec.getKeyBytes(gae)),
          codec.getValueBytes(gae));
    }
    catch(IOException ex) {
      throw new RuntimeException(ex);
    }
  }

  @Override
  public void beginWindow(long windowId)
  {
    currentWindowID = windowId;

    super.beginWindow(windowId);

    if(!readMetaData) {
      for(Long bucket: buckets) {
        byte[] windowIDValueBytes;

        windowIDValueBytes = load(bucket, WINDOW_ID_KEY);

        if(windowIDValueBytes == null) {
          continue;
        }

        long committedWindowID = GPOUtils.deserializeLong(windowIDValueBytes, new MutableInt(0));
        futureBuckets.put(bucket, committedWindowID);
      }

      //Write Store Format Version
      byte[] formatVersionValueBytes = GPOUtils.serializeInt(STORE_FORMAT_VERSION);

      for(Long bucket: buckets) {
        try {
          LOG.debug("Writing out store format version to bucket {}", bucket);
          put(bucket, STORE_FORMAT_KEY, formatVersionValueBytes);
        }
        catch(IOException ex) {
          throw new RuntimeException(ex);
        }
      }

      readMetaData = true;
    }
  }

  @Override
  protected void processEvent(AggregateEvent gae)
  {
    GPOMutable keys = gae.getKeys();
    GPOMutable aggregates = gae.getAggregates();

    int schemaID = gae.getSchemaID();
    int ddID = gae.getDimensionDescriptorID();
    int aggregatorID = gae.getAggregatorID();

    //Skip data for buckets with greater committed window Ids
    if(!futureBuckets.isEmpty()) {
      long bucket = getBucketForSchema(schemaID);
      Long committedWindowID = futureBuckets.get(bucket);

      if(committedWindowID != null &&
         currentWindowID <= committedWindowID) {
        LOG.debug("Skipping");
        return;
      }
    }

    FieldsDescriptor keyFieldsDescriptor = getKeyDescriptor(schemaID, ddID);
    FieldsDescriptor valueFieldsDescriptor = getValueDescriptor(schemaID, ddID, aggregatorID);

    keys.setFieldDescriptor(keyFieldsDescriptor);
    aggregates.setFieldDescriptor(valueFieldsDescriptor);

    DimensionsStaticAggregator aggregator = getAggregator(gae.getAggregatorID());

    AggregateEvent aggregate = cache.get(gae.getEventKey());

    if(aggregate == null) {
      aggregate = load(gae.getEventKey());

      if(aggregate != null) {
        cache.put(aggregate.getEventKey(), aggregate);
      }
      else {
        LOG.debug("Not in cache");
      }
    }

    if(aggregate == null) {
      cache.put(gae.getEventKey(), gae);
    }
    else {
      LOG.debug("Not in cache");
      aggregator.aggregateAggs(aggregate, gae);
    }
  }

  @Override
  public void endWindow()
  {
    byte[] currentWindowIDBytes = GPOUtils.serializeLong(currentWindowID);

    for(Long bucket: buckets) {
      Long committedWindowID = futureBuckets.get(bucket);

      if(committedWindowID == null ||
         committedWindowID <= currentWindowID) {
        futureBuckets.remove(bucket);

        try {
          put(bucket, WINDOW_ID_KEY, currentWindowIDBytes);
        }
        catch(IOException ex) {
          throw new RuntimeException(ex);
        }
      }
    }

    cacheWindowCount++;

    for(Map.Entry<EventKey, AggregateEvent> entry: cache.entrySet()) {
      putGAE(entry.getValue());
    }

    if(cacheWindowCount == cacheWindowDuration) {
      cache.clear();
      cacheWindowCount = 0;
    }

    super.endWindow();
  }

  @Override
  public HDHTCodec<AggregateEvent> getCodec()
  {
    return new GenericAggregateEventCodec();
  }

  /**
   * @return the cacheWindowDuration
   */
  public int getCacheWindowDuration()
  {
    return cacheWindowDuration;
  }

  /**
   * @param cacheWindowDuration the cacheWindowDuration to set
   */
  public void setCacheWindowDuration(int cacheWindowDuration)
  {
    this.cacheWindowDuration = cacheWindowDuration;
  }

  class GenericAggregateEventCodec extends KryoSerializableStreamCodec<AggregateEvent>
          implements HDHTCodec<AggregateEvent>
  {
    private static final long serialVersionUID = 201503170256L;

    public GenericAggregateEventCodec()
    {
    }

    @Override
    public byte[] getKeyBytes(AggregateEvent gae)
    {
      return getKeyBytesGAE(gae);
    }

    @Override
    public byte[] getValueBytes(AggregateEvent gae)
    {
      return getValueBytesGAE(gae);
    }

    @Override
    public AggregateEvent fromKeyValue(Slice key, byte[] value)
    {
      return fromKeyValueGAE(key, value);
    }

    @Override
    public int getPartition(AggregateEvent gae)
    {
      return getPartitionGAE(gae);
    }
  }

  @Override
  public void addQuery(HDSQuery query)
  {
    super.addQuery(query);
  }

  public ImmutableMap<Slice, HDSQuery> getQueries()
  {
    return ImmutableMap.copyOf(this.queries);
  }

  private static final Logger LOG = LoggerFactory.getLogger(DimensionsStoreHDHT.class);
}
