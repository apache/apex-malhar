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
import com.datatorrent.lib.appdata.qr.processor.QueryComputer;
import com.datatorrent.lib.appdata.qr.processor.QueryProcessor;
import com.datatorrent.lib.appdata.qr.processor.SimpleDoneQueryQueueManager;
import com.datatorrent.lib.appdata.schemas.FieldsDescriptor;
import com.datatorrent.lib.codec.KryoSerializableStreamCodec;
import com.google.common.base.Preconditions;
import com.google.common.cache.RemovalListener;
import com.google.common.cache.RemovalNotification;
import com.google.common.collect.Maps;
import com.google.common.primitives.Ints;
import java.io.IOException;
import javax.validation.constraints.Min;
import org.apache.commons.lang3.mutable.MutableBoolean;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

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

  public static final int DEFAULT_KEEP_ALIVE_TIME = 20;

  //HDHT Aggregation parameters
  @Min(1)
  private int keepAliveTime = DEFAULT_KEEP_ALIVE_TIME;

  protected transient Map<EventKey, GenericAggregateEvent> nonWaitingCache = Maps.newHashMap();
  private Map<EventKey, GenericAggregateEvent> waitingCache = Maps.newHashMap();

  private long windowID;

  private transient QueryProcessor<EventKey, HDSGenericEventQueryMeta, MutableBoolean, FetchResult, GenericAggregateEvent> cacheQueryProcessor;
  private long enqueueID = 0;

  public GenericDimensionsStoreHDHT()
  {
  }

  //TODO make all internal getters protected
  //timestamp prefix keys timestamp 0 if not needed but still there

  protected abstract DimensionsAggregator<GenericAggregateEvent> getAggregator(int aggregatorID);
  protected abstract FieldsDescriptor getKeyDescriptor(int schemaID, int dimensionsDescriptorID);
  protected abstract FieldsDescriptor getValueDescriptor(int schemaID, int dimensionsDescriptorID, int aggregatorID);
  protected abstract long getBucketForSchema(int schemaID);

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

    GenericAggregateEvent cachedGAE = nonWaitingCache.get(gae.getEventKey());

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
        cacheQueryProcessor.enqueue(gae.getEventKey(), null, new MutableBoolean(false));
      }
    }
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

    cacheQueryProcessor = new QueryProcessor(new GenericDimensionsFetchComputer(this),
                                             new GenericDimensionsFetchQueue(this));
    cacheQueryProcessor.setup(context);

    RemovalListener<EventKey, GenericAggregateEvent> removalListener = new RemovalListener<EventKey, GenericAggregateEvent>()
    {
      @Override
      public void onRemoval(RemovalNotification<EventKey, GenericAggregateEvent> notification)
      {
        GenericAggregateEvent gae = notification.getValue();
        putGAE(gae);
      }
    };

    //TODO reissue hdht queries for waiting cache entries.
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
    windowID = windowId;
    super.beginWindow(windowId);
    cacheQueryProcessor.beginWindow(windowId);
  }

  @Override
  public void endWindow()
  {
    FetchResult fetchResult = new FetchResult();
    fetchResult.setQueueDone(false);
    fetchResult.setQueryDone(false);

    while(true) {
      GenericAggregateEvent gae = cacheQueryProcessor.process(fetchResult);

      if(fetchResult.isQueueDone()) {
        break;
      }

      if(!fetchResult.isQueryDone()) {
        continue;
      }

      if(gae == null) {
        GenericAggregateEvent tgae = waitingCache.remove(fetchResult.getEventKey());
        nonWaitingCache.put(fetchResult.getEventKey(), tgae);
      }
      else {
        GenericAggregateEvent waitingCachedGAE = waitingCache.get(gae.getEventKey());
        DimensionsAggregator<GenericAggregateEvent> aggregator = getAggregator(gae.getAggregatorIndex());

        logger.info("Missing event {}, enqueueID {} {}", gae.getEventKey(), enqueueID, fetchResult.getEnqueueID());
        aggregator.aggregate(waitingCachedGAE, gae);
        waitingCache.remove(gae.getEventKey());
        nonWaitingCache.put(gae.getEventKey(), gae);
      }
    }

    for(GenericAggregateEvent cgae: nonWaitingCache.values()) {
      putGAE(cgae);
    }

    nonWaitingCache.clear();

    super.endWindow();

    cacheQueryProcessor.endWindow();
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
    private GenericDimensionsStoreHDHT operator;

    public GenericDimensionsFetchQueue(GenericDimensionsStoreHDHT operator)
    {
      setOperator(operator);
    }

    private void setOperator(GenericDimensionsStoreHDHT operator)
    {
      Preconditions.checkNotNull(operator);
      this.operator = operator;
    }

    @Override
    public boolean enqueue(EventKey query, HDSGenericEventQueryMeta metaQuery, MutableBoolean queueContext)
    {
      logger.info("WindowID: {} Enqueued: {}", windowID, enqueueID);
      Slice key = new Slice(getEventKeyBytesGAE(query));
      HDSQuery hdsQuery = operator.queries.get(key);

      if(hdsQuery == null) {
        hdsQuery = new HDSQuery();
        hdsQuery.bucketKey = getBucketForSchema(query.getSchemaID());
        hdsQuery.key = key;
        hdsQuery.keepAliveCount = 100;
        operator.addQuery(hdsQuery);
      }

      metaQuery = new HDSGenericEventQueryMeta(enqueueID);
      enqueueID++;
      metaQuery.setHDSQuery(hdsQuery);
      return super.enqueue(query, metaQuery, queueContext);
    }
  }

  class GenericDimensionsFetchComputer implements QueryComputer<EventKey, HDSGenericEventQueryMeta, MutableBoolean, FetchResult, GenericAggregateEvent>
  {
    private GenericDimensionsStoreHDHT operator;

    public GenericDimensionsFetchComputer(GenericDimensionsStoreHDHT operator)
    {
      setOperator(operator);
    }

    private void setOperator(GenericDimensionsStoreHDHT operator)
    {
      Preconditions.checkNotNull(operator);
      this.operator = operator;
    }

    @Override
    public GenericAggregateEvent processQuery(EventKey query,
                                              HDSGenericEventQueryMeta metaQuery,
                                              MutableBoolean queueContext,
                                              FetchResult context)
    {
      context.setEnqueueID(metaQuery.getEnqueueID());
      if(metaQuery.hdsQuery.processed) {
        context.setQueryDone(true);
        context.setEventKey(query);
        queueContext.setValue(true);

        if(metaQuery.hdsQuery.result != null) {
          return fromKeyValueGAE(metaQuery.hdsQuery.key, metaQuery.hdsQuery.result);
        }
      }
      else
      {
        context.setQueryDone(false);
      }

      return null;
    }

    @Override
    public void queueDepleted(FetchResult context)
    {
      context.setQueueDone(true);
    }
  }

  class FetchResult
  {
    private boolean queueDone;
    private boolean queryDone;
    private EventKey eventKey;
    private long enqueueID;

    public FetchResult()
    {
    }

    public boolean isQueueDone()
    {
      return queueDone;
    }

    public void setQueueDone(boolean queueDone)
    {
      this.queueDone = queueDone;
    }

    public boolean isQueryDone()
    {
      return queryDone;
    }

    public void setQueryDone(boolean queryDone)
    {
      this.queryDone = queryDone;
    }

    /**
     * @return the eventKey
     */
    public EventKey getEventKey()
    {
      return eventKey;
    }

    /**
     * @param eventKey the eventKey to set
     */
    public void setEventKey(EventKey eventKey)
    {
      this.eventKey = eventKey;
    }

    /**
     * @param enqueueID the enqueueID to set
     */
    public void setEnqueueID(long enqueueID)
    {
      this.enqueueID = enqueueID;
    }

    /**
     * @return the enqueueID
     */
    public long getEnqueueID()
    {
      return enqueueID;
    }
  }

  public static class HDSGenericEventQueryMeta
  {
    private HDSQuery hdsQuery;
    private long enqueueID;

    public HDSGenericEventQueryMeta(long enqueueID)
    {
      this.enqueueID = enqueueID;
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

    /**
     * @return the enqueueID
     */
    public long getEnqueueID()
    {
      return enqueueID;
    }
  }
}
