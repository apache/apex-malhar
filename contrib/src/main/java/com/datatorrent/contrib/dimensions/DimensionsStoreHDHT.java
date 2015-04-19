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

import com.datatorrent.api.Context.OperatorContext;
import com.datatorrent.api.annotation.OperatorAnnotation;
import com.datatorrent.common.util.Slice;
import com.datatorrent.contrib.hdht.AbstractSinglePortHDHTWriter;
import com.datatorrent.lib.appdata.dimensions.AggregateEvent;
import com.datatorrent.lib.appdata.dimensions.AggregateEvent.EventKey;
import com.datatorrent.lib.appdata.dimensions.DimensionsStaticAggregator;
import com.datatorrent.lib.appdata.dimensions.DimensionsDescriptor;
import com.datatorrent.lib.appdata.gpo.GPOByteArrayList;
import com.datatorrent.lib.appdata.gpo.GPOImmutable;
import com.datatorrent.lib.appdata.gpo.GPOMutable;
import com.datatorrent.lib.appdata.gpo.GPOUtils;
import com.datatorrent.lib.appdata.schemas.FieldsDescriptor;
import com.datatorrent.lib.codec.KryoSerializableStreamCodec;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.cache.RemovalListener;
import com.google.common.cache.RemovalNotification;
import com.google.common.primitives.Ints;
import com.google.common.primitives.Longs;
import java.io.IOException;
import javax.validation.constraints.Min;
import org.apache.commons.lang3.mutable.MutableInt;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.ExecutionException;

/**
 * TODO aggregate by windowID in waiting cache.
 */
@OperatorAnnotation(checkpointableWithinAppWindow=false)
public abstract class DimensionsStoreHDHT extends AbstractSinglePortHDHTWriter<AggregateEvent>
{
  private static final Logger logger = LoggerFactory.getLogger(DimensionsStoreHDHT.class);

  public static final int CACHE_SIZE = 50000;
  public static final int DEFAULT_KEEP_ALIVE_TIME = 20;

  //HDHT Aggregation parameters
  @Min(1)
  private int keepAliveTime = DEFAULT_KEEP_ALIVE_TIME;

  ////////////////////// Caching /////////////////////////////

  //TODO this is not fault tolerant. Need to create a custom cache which has old entries removed in end window.
  //Can't write out incomplete aggregates in the middle of a window.
  private int cacheSize = CACHE_SIZE;
  protected transient LoadingCache<EventKey, AggregateEvent> cache = null;

  ////////////////////// Caching /////////////////////////////

  public DimensionsStoreHDHT()
  {
  }

  //TODO make all internal getters protected
  //timestamp prefix keys timestamp 0 if not needed but still there

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
            getFieldDescriptor().
            getFields().getFields().
            contains(DimensionsDescriptor.DIMENSION_TIME)) {
      timestamp = eventKey.getKey().getFieldLong(DimensionsDescriptor.DIMENSION_TIME);
    }

    byte[] timeBytes = Longs.toByteArray(timestamp);
    byte[] schemaIDBytes = Ints.toByteArray(eventKey.getSchemaID());
    byte[] dimensionDescriptorIDBytes = Ints.toByteArray(eventKey.getDimensionDescriptorID());
    byte[] aggregatorIDBytes = Ints.toByteArray(eventKey.getAggregatorIndex());
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

    AggregateEvent gae = new AggregateEvent(new GPOImmutable(keys),
                                                          aggs,
                                                          schemaID,
                                                          dimensionDescriptorID,
                                                          aggregatorID);
    return gae;
  }

  @Override
  protected void processEvent(AggregateEvent gae)
  {
    GPOImmutable keys = gae.getKeys();
    GPOMutable aggregates = gae.getAggregates();

    int schemaID = gae.getSchemaID();
    int ddID = gae.getDimensionDescriptorID();
    int aggregatorID = gae.getAggregatorIndex();

    FieldsDescriptor keyFieldsDescriptor = getKeyDescriptor(schemaID, ddID);
    FieldsDescriptor valueFieldsDescriptor = getValueDescriptor(schemaID, ddID, aggregatorID);

    keys.setFieldDescriptor(keyFieldsDescriptor);
    aggregates.setFieldDescriptor(valueFieldsDescriptor);

    processGenericEvent(gae);
  }

  protected void processGenericEvent(AggregateEvent gae)
  {
    DimensionsStaticAggregator aggregator = getAggregator(gae.getAggregatorIndex());
    AggregateEvent aggregate = null;

    try {
      aggregate = cache.get(gae.getEventKey());
    }
    catch(ExecutionException ex) {
      throw new RuntimeException(ex);
    }

    if(aggregate.isEmpty()) {
      cache.put(gae.getEventKey(), gae);
    }
    else {
      aggregator.aggregate(aggregate, gae);
    }
  }

  public abstract int getPartitionGAE(AggregateEvent inputEvent);

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
  public void setup(OperatorContext context)
  {
    super.setup(context);

    cache = CacheBuilder.newBuilder()
         .maximumSize(getCacheSize())
         .removalListener(new HDHTCacheRemoval())
         .build(new HDHTCacheLoader());

    //TODO reissue hdht queries for waiting cache entries.
  }

  @Override
  public void teardown()
  {
    super.teardown();
  }

  @Override
  public void beginWindow(long windowId)
  {
    super.beginWindow(windowId);
  }

  @Override
  public void endWindow()
  {
    for(Map.Entry<EventKey, AggregateEvent> entry: cache.asMap().entrySet()) {
      AggregateEvent gae = entry.getValue();

      if(gae == null || gae.isEmpty()) {
        continue;
      }

      try {
        super.processEvent(entry.getValue());
      }
      catch(IOException ex) {
        throw new RuntimeException(ex);
      }
    }

    super.endWindow();
  }

  @Override
  public HDHTCodec<AggregateEvent> getCodec()
  {
    return new GenericAggregateEventCodec();
  }

  /**
   * @return the keepAliveTime
   */
  public int getKeepAliveTime()
  {
    return keepAliveTime;
  }

  /**
   * @param keepAliveTime the keepAliveTime to set
   */
  public void setKeepAliveTime(int keepAliveTime)
  {
    this.keepAliveTime = keepAliveTime;
  }

  /**
   * @return the cacheSize
   */
  public int getCacheSize()
  {
    return cacheSize;
  }

  /**
   * @param cacheSize the cacheSize to set
   */
  public void setCacheSize(int cacheSize)
  {
    this.cacheSize = cacheSize;
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

  class HDHTCacheRemoval implements RemovalListener<EventKey, AggregateEvent>
  {
    public HDHTCacheRemoval()
    {
    }

    @Override
    public void onRemoval(RemovalNotification<EventKey, AggregateEvent> notification)
    {
      AggregateEvent gae = notification.getValue();

      if(gae.isEmpty()) {
        return;
      }

      putGAE(gae);
    }
  }

  class HDHTCacheLoader extends CacheLoader<EventKey, AggregateEvent>
  {
    public HDHTCacheLoader()
    {
    }

    @Override
    public AggregateEvent load(EventKey eventKey)
    {
      long bucket = getBucketForSchema(eventKey);
      byte[] key = getEventKeyBytesGAE(eventKey);

      Slice keySlice = new Slice(key, 0, key.length);
      byte[] val = getUncommitted(bucket, keySlice);

      if(val == null) {
        try {
          val = get(bucket, keySlice);
        }
        catch(IOException ex) {
          throw new RuntimeException(ex);
        }
      }

     if(val == null) {
       return new AggregateEvent();
     }

     return fromKeyValueGAE(keySlice, val);
    }
  }
}
