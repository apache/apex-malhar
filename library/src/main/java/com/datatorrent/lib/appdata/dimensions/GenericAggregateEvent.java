/*
 *  Copyright (c) 2012-2015 Malhar, Inc.
 *  All Rights Reserved.
 */

package com.datatorrent.lib.appdata.dimensions;

import com.datatorrent.lib.appdata.gpo.GPOImmutable;
import com.datatorrent.lib.appdata.gpo.GPOMutable;
import com.datatorrent.lib.statistics.DimensionsComputation;
import com.google.common.base.Preconditions;

/**
 *
 * @author Timothy Farkas: tim@datatorrent.com
 */
public class GenericAggregateEvent implements DimensionsComputation.AggregateEvent
{
  private GPOImmutable keys;
  private GPOMutable aggregates;
  private int aggregatorIndex;

  public GenericAggregateEvent(GPOImmutable keys,
                               GPOMutable aggregates,
                               int aggregatorIndex)
  {
    setKeys(keys);
    setAggregates(aggregates);
    this.aggregatorIndex = aggregatorIndex;
  }

  private void setKeys(GPOImmutable keys)
  {
    Preconditions.checkNotNull(keys);
    this.keys = keys;
  }

  public GPOImmutable getKeys()
  {
    return keys;
  }

  private void setAggregates(GPOMutable aggregates)
  {
    Preconditions.checkNotNull(aggregates);
    this.aggregates = aggregates;
  }

  public GPOMutable getAggregates()
  {
    return aggregates;
  }

  @Override
  public int getAggregatorIndex()
  {
    return aggregatorIndex;
  }
}
