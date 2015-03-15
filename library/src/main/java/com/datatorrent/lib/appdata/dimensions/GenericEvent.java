/*
 *  Copyright (c) 2012-2015 Malhar, Inc.
 *  All Rights Reserved.
 */

package com.datatorrent.lib.appdata.dimensions;

import com.datatorrent.lib.appdata.gpo.GPOImmutable;
import com.google.common.base.Preconditions;

/**
 *
 * @author Timothy Farkas: tim@datatorrent.com
 */
public class GenericEvent
{
  private GPOImmutable keys;
  private GPOImmutable aggregates;

  public GenericEvent(GPOImmutable keys,
                      GPOImmutable aggregates)
  {
    setKeys(keys);
    setAggregates(aggregates);
  }

  private void setKeys(GPOImmutable keys)
  {
    Preconditions.checkNotNull(keys);
    this.keys = keys;
  }

  private void setAggregates(GPOImmutable aggregates)
  {
    Preconditions.checkNotNull(aggregates);
    this.aggregates = aggregates;
  }

  public GPOImmutable getKeys()
  {
    return keys;
  }

  public GPOImmutable getAggregates()
  {
    return aggregates;
  }
}
