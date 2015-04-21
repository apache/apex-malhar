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
package com.datatorrent.lib.appdata.dimensions;

import com.datatorrent.api.BaseOperator;
import com.datatorrent.api.Context.OperatorContext;
import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.api.Operator;
import com.datatorrent.api.annotation.OperatorAnnotation;
import com.datatorrent.lib.appdata.dimensions.AggregateEvent.EventKey;
import com.datatorrent.lib.appdata.gpo.GPOMutable;
import com.datatorrent.lib.appdata.schemas.FieldsDescriptor;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.RemovalListener;
import com.google.common.cache.RemovalNotification;
import com.google.common.collect.Lists;
import javax.validation.constraints.Min;

import java.util.List;
import java.util.Map;

@OperatorAnnotation(checkpointableWithinAppWindow=false)
public abstract class DimensionsComputation<INPUT_EVENT> implements Operator
{
  public static final long DEFAULT_CACHE_SIZE = 50000;

  @Min(1)
  private long cacheSize = DEFAULT_CACHE_SIZE;

  @VisibleForTesting
  public transient Cache<EventKey, AggregateEvent> cache =
  CacheBuilder.newBuilder().maximumSize(cacheSize).removalListener(new CacheRemovalListener()).build();

  private transient List<AggregateEvent> aggregateEventBuffer = Lists.newArrayList();

  public transient final DefaultInputPort<INPUT_EVENT> inputEvent = new DefaultInputPort<INPUT_EVENT>() {
    @Override
    public void process(INPUT_EVENT tuple)
    {
      processInputEvent(tuple);
    }
  };

  public final transient DefaultOutputPort<AggregateEvent> aggregateOutput = new DefaultOutputPort<AggregateEvent>();

  public DimensionsComputation()
  {
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
  public void setup(OperatorContext context)
  {
  }

  @Override
  public void beginWindow(long windowId)
  {
  }

  @Override
  public void endWindow()
  {
    cache.invalidateAll();
  }

  @Override
  public void teardown()
  {
  }

  public abstract void convertInputEvent(INPUT_EVENT inputEvent, List<AggregateEvent> aggregateEventBuffer);
  public abstract FieldsDescriptor getAggregateFieldsDescriptor(int schemaID,
                                                                int dimensionDescriptorID,
                                                                int aggregatorID);

  public AggregatorInfo getAggregatorInfo()
  {
    return AggregatorUtils.DEFAULT_AGGREGATOR_INFO;
  }

  public void processInputEvent(INPUT_EVENT inputEvent)
  {
    convertInputEvent(inputEvent, aggregateEventBuffer);

    for(AggregateEvent gae: aggregateEventBuffer) {
      processGenericEvent(gae);
    }

    aggregateEventBuffer.clear();
  }

  public void processGenericEvent(AggregateEvent gae)
  {
    DimensionsStaticAggregator aggregator = getAggregatorInfo().getStaticAggregatorIDToAggregator().get(gae.getAggregatorID());
    AggregateEvent aggregate = cache.getIfPresent(gae.getEventKey());

    if(aggregate == null) {
      gae = aggregator.createDest(gae,
                                  getAggregateFieldsDescriptor(gae.getSchemaID(),
                                                               gae.getDimensionDescriptorID(),
                                                               gae.getAggregatorID()));
      cache.put(gae.getEventKey(), gae);
      return;
    }

    aggregator.aggregate(aggregate, gae);

    AggregateEvent newAggregate = new AggregateEvent(aggregate.getEventKey(),
                                                                   new GPOMutable(aggregate.getAggregates()));
    cache.put(newAggregate.getEventKey(), newAggregate);
  }

  class CacheRemovalListener implements RemovalListener<EventKey, AggregateEvent>
  {
    public CacheRemovalListener()
    {
    }

    @Override
    public void onRemoval(RemovalNotification<EventKey, AggregateEvent> notification)
    {
      aggregateOutput.emit(notification.getValue());
    }
  }

  @OperatorAnnotation(checkpointableWithinAppWindow=false)
  class DimensionsComputationUnifier extends BaseOperator implements Operator.Unifier<AggregateEvent>
  {
    public static final long DEFAULT_CACHE_SIZE = 50000;

    public final transient DefaultOutputPort<AggregateEvent> output = new DefaultOutputPort<AggregateEvent>();

    @Min(1)
    private long cacheSize = DEFAULT_CACHE_SIZE;

    private transient Cache<EventKey, AggregateEvent> cache =
    CacheBuilder.newBuilder().maximumSize(getCacheSize()).removalListener(new CacheRemovalListener()).build();

    private Map<Integer, DimensionsStaticAggregator> aggregatorIDToAggregator;

    public DimensionsComputationUnifier(Map<Integer, DimensionsStaticAggregator> aggregatorIDToAggregator)
    {
      setAggregatorIDToAggregator(aggregatorIDToAggregator);
    }

    private void setAggregatorIDToAggregator(Map<Integer, DimensionsStaticAggregator> aggregatorIDToAggregator)
    {
      this.aggregatorIDToAggregator = Preconditions.checkNotNull(aggregatorIDToAggregator,
                                                                 "aggregatorIDToAggregator");
    }

    @Override
    public void process(AggregateEvent srcAgg)
    {
      DimensionsStaticAggregator aggregator = aggregatorIDToAggregator.get(srcAgg.getAggregatorID());
      AggregateEvent destAgg = cache.getIfPresent(srcAgg.getEventKey());

      if(destAgg == null) {
        cache.put(srcAgg.getEventKey(), srcAgg);
        return;
      }

      aggregator.aggregate(destAgg, srcAgg);
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

    class CacheRemovalListener implements RemovalListener<EventKey, AggregateEvent>
    {
      public CacheRemovalListener()
      {
      }

      @Override
      public void onRemoval(RemovalNotification<EventKey, AggregateEvent> notification)
      {
        output.emit(notification.getValue());
      }
    }
  }
}
