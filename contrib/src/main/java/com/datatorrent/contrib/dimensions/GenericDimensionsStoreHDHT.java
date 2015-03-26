/*
 *  Copyright (c) 2012-2015 Malhar, Inc.
 *  All Rights Reserved.
 */

package com.datatorrent.contrib.dimensions;

import com.datatorrent.api.Context.OperatorContext;
import com.datatorrent.api.annotation.OperatorAnnotation;
import com.datatorrent.common.util.Slice;
import com.datatorrent.contrib.hdht.AbstractSinglePortHDHTWriter;
import com.datatorrent.lib.appdata.dimensions.DimensionsAggregator;
import com.datatorrent.lib.appdata.dimensions.GenericAggregateEvent;
import com.datatorrent.lib.appdata.dimensions.GenericAggregateEvent.EventKey;
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
import java.io.IOException;
import javax.validation.constraints.Min;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;


/**
 *
 * @author Timothy Farkas: tim@datatorrent.com
 *
 * TODO aggregate by windowID in waiting cache.
 */
@OperatorAnnotation(checkpointableWithinAppWindow=false)
public abstract class GenericDimensionsStoreHDHT extends AbstractSinglePortHDHTWriter<GenericAggregateEvent>
{
  private static final Logger logger = LoggerFactory.getLogger(GenericDimensionsStoreHDHT.class);

  public static final int CACHE_SIZE = 50000;
  public static final int DEFAULT_KEEP_ALIVE_TIME = 20;

  //HDHT Aggregation parameters
  @Min(1)
  private int keepAliveTime = DEFAULT_KEEP_ALIVE_TIME;

  ////////////////////// Caching /////////////////////////////

  private int cacheSize = CACHE_SIZE;
  private transient LoadingCache<EventKey, GenericAggregateEvent> cache = null;

  ////////////////////// Caching /////////////////////////////

  public GenericDimensionsStoreHDHT()
  {
  }

  //TODO make all internal getters protected
  //timestamp prefix keys timestamp 0 if not needed but still there

  protected abstract DimensionsAggregator<GenericAggregateEvent> getAggregator(int aggregatorID);
  protected abstract FieldsDescriptor getKeyDescriptor(int schemaID, int dimensionsDescriptorID);
  protected abstract FieldsDescriptor getValueDescriptor(int schemaID, int dimensionsDescriptorID, int aggregatorID);
  protected abstract long getBucketForSchema(int schemaID);

  protected long getBucketForSchema(EventKey eventKey)
  {
    return getBucketForSchema(eventKey.getSchemaID());
  }

  protected byte[] getKeyBytesGAE(GenericAggregateEvent gae)
  {
    return getEventKeyBytesGAE(gae.getEventKey());
  }

  public byte[] getEventKeyBytesGAE(EventKey eventKey)
  {
    byte[] schemaIDBytes = Ints.toByteArray(eventKey.getSchemaID());
    byte[] dimensionDescriptorIDBytes = Ints.toByteArray(eventKey.getDimensionDescriptorID());
    byte[] aggregatorIDBytes = Ints.toByteArray(eventKey.getAggregatorIndex());
    byte[] gpoBytes = GPOUtils.serialize(eventKey.getKey());

    GPOByteArrayList bal = new GPOByteArrayList();
    bal.add(schemaIDBytes);
    bal.add(dimensionDescriptorIDBytes);
    bal.add(aggregatorIDBytes);
    bal.add(gpoBytes);

    return bal.toByteArray();
  }

  public byte[] getValueBytesGAE(GenericAggregateEvent event)
  {
    return GPOUtils.serialize(event.getAggregates());
  }

  public GenericAggregateEvent fromKeyValueGAE(Slice key, byte[] aggregate)
  {
    int offset = 0;
    int schemaID = GPOUtils.deserializeInt(key.buffer,
                                           offset);
    offset += 4;
    int dimensionDescriptorID = GPOUtils.deserializeInt(key.buffer,
                                                        offset);
    offset += 4;
    int aggregatorID = GPOUtils.deserializeInt(key.buffer,
                                               offset);
    offset += 4;

    FieldsDescriptor keysDescriptor = getKeyDescriptor(schemaID, dimensionDescriptorID);
    FieldsDescriptor aggDescriptor = getValueDescriptor(schemaID, dimensionDescriptorID, aggregatorID);

    GPOMutable keys = GPOUtils.deserialize(keysDescriptor, key.buffer, offset);
    GPOMutable aggs = GPOUtils.deserialize(aggDescriptor, aggregate, 0);

    GenericAggregateEvent gae = new GenericAggregateEvent(new GPOImmutable(keys),
                                                          aggs,
                                                          schemaID,
                                                          dimensionDescriptorID,
                                                          aggregatorID);
    return gae;
  }

  @Override
  protected void processEvent(GenericAggregateEvent gae)
  {
    processGenericEvent(gae);
  }

  protected void processGenericEvent(GenericAggregateEvent gae)
  {
    DimensionsAggregator<GenericAggregateEvent> aggregator = getAggregator(gae.getAggregatorIndex());
    GenericAggregateEvent aggregate = null;

    try {
      aggregate = cache.get(gae.getEventKey());
    }
    catch(ExecutionException ex) {
      throw new RuntimeException(ex);
    }

    aggregator.aggregate(aggregate, gae);
  }

  public abstract int getPartitionGAE(GenericAggregateEvent inputEvent);

  public void putGAE(GenericAggregateEvent gae)
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
         .maximumSize(cacheSize)
         .removalListener(new HDHTCacheRemoval())
         .expireAfterWrite(5, TimeUnit.MINUTES)
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
    super.endWindow();
  }

  @Override
  public HDHTCodec<GenericAggregateEvent> getCodec()
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

  class GenericAggregateEventCodec extends KryoSerializableStreamCodec<GenericAggregateEvent>
          implements HDHTCodec<GenericAggregateEvent>
  {
    private static final long serialVersionUID = 201503170256L;

    public GenericAggregateEventCodec()
    {
    }

    @Override
    public byte[] getKeyBytes(GenericAggregateEvent gae)
    {
      return getKeyBytesGAE(gae);
    }

    @Override
    public byte[] getValueBytes(GenericAggregateEvent gae)
    {
      return getValueBytesGAE(gae);
    }

    @Override
    public GenericAggregateEvent fromKeyValue(Slice key, byte[] value)
    {
      return fromKeyValueGAE(key, value);
    }

    @Override
    public int getPartition(GenericAggregateEvent gae)
    {
      return getPartitionGAE(gae);
    }
  }

  class HDHTCacheRemoval implements RemovalListener<EventKey, GenericAggregateEvent>
  {
    public HDHTCacheRemoval()
    {
    }

    @Override
    public void onRemoval(RemovalNotification<EventKey, GenericAggregateEvent> notification)
    {
      GenericAggregateEvent gae = notification.getValue();
      putGAE(gae);
    }
  }

  class HDHTCacheLoader extends CacheLoader<EventKey, GenericAggregateEvent>
  {
    public HDHTCacheLoader()
    {
    }

    @Override
    public GenericAggregateEvent load(EventKey eventKey) throws Exception
    {
      long bucket = getBucketForSchema(eventKey);
      byte[] key = getEventKeyBytesGAE(eventKey);
      if(key == null) {
        return null;
      }

      Slice keySlice = new Slice(key, 0, key.length);
      byte[] val = getUncommitted(bucket, keySlice);

      if(val == null) {
        val = get(bucket, keySlice);
      }

     if (val == null)
       return null;

     return fromKeyValueGAE(keySlice, val);
    }
  }
}
