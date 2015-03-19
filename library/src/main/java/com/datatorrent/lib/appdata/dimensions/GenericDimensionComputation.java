/*
 *  Copyright (c) 2012-2015 Malhar, Inc.
 *  All Rights Reserved.
 */

package com.datatorrent.lib.appdata.dimensions;

import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.api.Operator;

/**
 *
 * @author Timothy Farkas: tim@datatorrent.com
 */
public abstract class GenericDimensionComputation<INPUT_EVENT> implements Operator
{
  public static long DEFAULT_CACHE_SIZE = 50000;

  private long cacheSize = DEFAULT_CACHE_SIZE;

  /*
  private transient Cache<EventKey, GenericAggregateEvent> nonWaitingCache =
  CacheBuilder.newBuilder().maximumSize(cacheSize).removalListener(null);
  */
  
  public transient final DefaultInputPort<INPUT_EVENT> inputEvent = new DefaultInputPort<INPUT_EVENT>() {
    @Override
    public void process(INPUT_EVENT tuple)
    {
      processInputEvent(tuple);
    }
  };

  public final transient DefaultOutputPort<String> aggregateOutput = new DefaultOutputPort<String>();

  public GenericDimensionComputation()
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

  public abstract GenericAggregateEvent[] convertInputEvent(INPUT_EVENT inputEvent);
  public abstract DimensionsAggregator<GenericAggregateEvent> getAggregator(int aggregatorID);

  public void processInputEvent(INPUT_EVENT inputEvent)
  {
    GenericAggregateEvent[] gaes = convertInputEvent(inputEvent);

    for(GenericAggregateEvent gae: gaes) {

    }
  }

  public void processGenericEvent()
  {
  }
}
