/*
 *  Copyright (c) 2012-2015 Malhar, Inc.
 *  All Rights Reserved.
 */

package com.datatorrent.contrib.dimensions;

import com.datatorrent.api.Context.OperatorContext;
import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.api.annotation.OperatorAnnotation;
import com.datatorrent.common.util.DTThrowable;
import com.datatorrent.common.util.Slice;
import com.datatorrent.contrib.hdht.AbstractSinglePortHDHTWriter;
import com.datatorrent.lib.appdata.dimensions.DimensionsAggregator;
import com.datatorrent.lib.appdata.dimensions.GenericAggregateEvent;
import com.datatorrent.lib.appdata.dimensions.GenericAggregateEvent.EventKey;
import com.datatorrent.lib.appdata.dimensions.GenericAggregateEventUtils;
import com.datatorrent.lib.appdata.gpo.GPOByteArrayList;
import com.datatorrent.lib.appdata.gpo.GPOUtils;
import com.datatorrent.lib.appdata.qr.processor.QueryComputer;
import com.datatorrent.lib.appdata.qr.processor.QueryProcessor;
import com.datatorrent.lib.appdata.qr.processor.SimpleDoneQueryQueueManager;
import com.datatorrent.lib.appdata.schemas.FieldsDescriptor;
import com.datatorrent.lib.codec.KryoSerializableStreamCodec;
import com.google.common.base.Preconditions;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.RemovalListener;
import com.google.common.cache.RemovalNotification;
import com.google.common.primitives.Ints;
import java.io.IOException;
import javax.validation.constraints.Min;
import org.apache.commons.lang3.mutable.MutableBoolean;

import java.util.Map;

/**
 *
 * @author Timothy Farkas: tim@datatorrent.com
 */
@OperatorAnnotation(checkpointableWithinAppWindow=false)
public abstract class GenericDimensionsStoreAIHDHT<INPUT_EVENT> extends AbstractSinglePortHDHTWriter<GenericAggregateEvent>
{
  public static final long DEFAULT_CACHE_SIZE = 10000;
  public static final int DEFAULT_KEEP_ALIVE_TIME = 20;

  //HDHT Aggregation parameters
  @Min(1)
  private long cacheSize = DEFAULT_CACHE_SIZE;
  @Min(1)
  private int keepAliveTime = DEFAULT_KEEP_ALIVE_TIME;

  private transient Cache<EventKey, GenericAggregateEvent> nonWaitingCache;
  private transient Map<EventKey, GenericAggregateEvent> waitingCache;

  private transient QueryProcessor<EventKey, HDSGenericEventQueryMeta, MutableBoolean, MutableBoolean, GenericAggregateEvent> cacheQueryProcessor;

  public transient final DefaultInputPort<INPUT_EVENT> inputEvent = new DefaultInputPort<INPUT_EVENT>() {
    @Override
    public void process(INPUT_EVENT tuple)
    {
      processInputEvent(tuple);
    }
  };

  public GenericDimensionsStoreAIHDHT()
  {
  }

  public abstract DimensionsAggregator<GenericAggregateEvent> getAggregator(int aggregatorID);
  public abstract FieldsDescriptor getKeyDescriptor(int schemaID, int dimensionsDescriptorID);
  public abstract FieldsDescriptor getValueDescriptor(int schemaID, int dimensionsDescriptorID, int aggregatorID);
  public abstract long getBucketForSchema(int schemaID);

  public byte[] getKeyBytesGAE(GenericAggregateEvent gae)
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
    int schemaID = GPOUtils.deserializeInt(aggregate,
                                           0);
    int dimensionDescriptorID = GPOUtils.deserializeInt(aggregate,
                                                        4);
    int aggregatorID = GPOUtils.deserializeInt(aggregate,
                                                8);

    FieldsDescriptor keysDescriptor = getKeyDescriptor(schemaID, dimensionDescriptorID);
    FieldsDescriptor aggDescriptor = getValueDescriptor(schemaID, dimensionDescriptorID, aggregatorID);

    return GenericAggregateEventUtils.deserialize(aggregate, keysDescriptor, aggDescriptor);
  }

  protected GenericAggregateEvent[] convertInput(INPUT_EVENT tuple)
  {
    throw new UnsupportedOperationException("This method must be implemented.");
  }

  protected void processInputEvent(INPUT_EVENT tuple)
  {
    GenericAggregateEvent[] gaes = convertInput(tuple);

    for(GenericAggregateEvent gae: gaes) {
      processGenericEvent(gae);
    }
  }

  protected void processGenericEvent(GenericAggregateEvent gae)
  {
    DimensionsAggregator<GenericAggregateEvent> aggregator = getAggregator(gae.getAggregatorIndex());

    GenericAggregateEvent cachedGAE = nonWaitingCache.getIfPresent(gae.getEventKey());

    if(cachedGAE != null) {
      aggregator.aggregate(cachedGAE, gae);
    }
    else {
      GenericAggregateEvent waitingCachedGAE = waitingCache.get(gae.getEventKey());

      if(waitingCachedGAE != null) {
        aggregator.aggregate(waitingCachedGAE, gae);
      }
      else {
        waitingCache.put(gae.getEventKey(), gae);
        cacheQueryProcessor.enqueue(gae.getEventKey(), new HDSGenericEventQueryMeta(), new MutableBoolean(false));
      }
    }
  }

  public abstract int getPartitionGAE(GenericAggregateEvent inputEvent);

  @Override
  public void setup(OperatorContext context)
  {
    super.setup(context);

    cacheQueryProcessor = new QueryProcessor(new GenericDimensionsFetchComputer(this),
                                             new GenericDimensionsFetchQueue(this));
    cacheQueryProcessor.setup(context);

    RemovalListener<EventKey, GenericAggregateEvent> removalListener = new RemovalListener<EventKey, GenericAggregateEvent>()
    {
      @Override
      public void onRemoval(RemovalNotification<EventKey, GenericAggregateEvent> notification)
      {
        GenericAggregateEvent gae = notification.getValue();

        try {
          put(getBucketForSchema(gae.getSchemaID()),
            new Slice(codec.getKeyBytes(gae)),
            codec.getValueBytes(gae));
        }
        catch(IOException ex) {
          DTThrowable.rethrow(ex);
        }
      }
    };

    nonWaitingCache = CacheBuilder.newBuilder().
                      maximumSize(cacheSize).
                      removalListener(removalListener).
                      build();
  }

  @Override
  public void teardown()
  {
    cacheQueryProcessor.teardown();
    super.teardown();
  }

  @Override
  public void beginWindow(long windowId)
  {
    super.beginWindow(windowId);
    cacheQueryProcessor.beginWindow(windowId);
  }

  @Override
  public void endWindow()
  {
    MutableBoolean done = new MutableBoolean(false);

    while(done.isFalse()) {
      GenericAggregateEvent gae = cacheQueryProcessor.process(done);

      if(gae == null) {
        continue;
      }

      GenericAggregateEvent waitingCachedGAE = waitingCache.get(gae.getEventKey());
      DimensionsAggregator<GenericAggregateEvent> aggregator = getAggregator(gae.getAggregatorIndex());

      aggregator.aggregate(waitingCachedGAE, gae);
      waitingCache.remove(gae.getEventKey());
      nonWaitingCache.put(gae.getEventKey(), gae);
    }

    super.endWindow();

    cacheQueryProcessor.endWindow();
  }

  /**
   * @return the cacheSize
   */
  public long getCacheSize()
  {
    return cacheSize;
  }

  /**
   * @param cacheSize the cacheSize to set
   */
  public void setCacheSize(long cacheSize)
  {
    this.cacheSize = cacheSize;
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

  class GenericDimensionsFetchQueue extends SimpleDoneQueryQueueManager<EventKey, HDSGenericEventQueryMeta>
  {
    private GenericDimensionsStoreAIHDHT<INPUT_EVENT> operator;

    public GenericDimensionsFetchQueue(GenericDimensionsStoreAIHDHT<INPUT_EVENT> operator)
    {
      setOperator(operator);
    }

    private void setOperator(GenericDimensionsStoreAIHDHT<INPUT_EVENT> operator)
    {
      Preconditions.checkNotNull(operator);
      this.operator = operator;
    }

    @Override
    public boolean enqueue(EventKey query, HDSGenericEventQueryMeta metaQuery, MutableBoolean queueContext)
    {
      Slice key = new Slice(getEventKeyBytesGAE(query));
      HDSQuery hdsQuery = operator.queries.get(key);

      if(hdsQuery == null) {
        hdsQuery = new HDSQuery();
        hdsQuery.bucketKey = getBucketForSchema(query.getSchemaID());
        hdsQuery.key = key;
        hdsQuery.keepAliveCount = operator.keepAliveTime;
        operator.addQuery(hdsQuery);
      }

      return true;
    }
  }

  class GenericDimensionsFetchComputer implements QueryComputer<EventKey, HDSGenericEventQueryMeta, MutableBoolean, MutableBoolean, GenericAggregateEvent>
  {
    private GenericDimensionsStoreAIHDHT<INPUT_EVENT> operator;

    public GenericDimensionsFetchComputer(GenericDimensionsStoreAIHDHT<INPUT_EVENT> operator)
    {
      setOperator(operator);
    }

    private void setOperator(GenericDimensionsStoreAIHDHT<INPUT_EVENT> operator)
    {
      Preconditions.checkNotNull(operator);
      this.operator = operator;
    }

    @Override
    public GenericAggregateEvent processQuery(EventKey query,
                                              HDSGenericEventQueryMeta metaQuery,
                                              MutableBoolean queueContext,
                                              MutableBoolean context)
    {
      if(metaQuery.hdsQuery.processed &&
         metaQuery.hdsQuery.result != null) {
        return fromKeyValueGAE(metaQuery.hdsQuery.key, metaQuery.hdsQuery.result);
      }

      return null;
    }

    @Override
    public void queueDepleted(MutableBoolean context)
    {
      context.setValue(true);
    }
  }

  public static class HDSGenericEventQueryMeta
  {
    private HDSQuery hdsQuery;

    public HDSGenericEventQueryMeta()
    {
    }

    public void setHDSQuery(HDSQuery hdsQuery)
    {
      Preconditions.checkNotNull(hdsQuery);
      this.hdsQuery = hdsQuery;
    }

    public HDSQuery getHDSQuery()
    {
      return hdsQuery;
    }
  }
}
