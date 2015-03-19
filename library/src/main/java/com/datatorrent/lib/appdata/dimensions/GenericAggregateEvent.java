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
  private long schemaID;
  private int aggregatorIndex;

  private GPOImmutable keys;
  private GPOMutable aggregates;
  private EventKey eventKey;

  public GenericAggregateEvent(GPOImmutable keys,
                               GPOMutable aggregates,
                               long schemaID,
                               int aggregatorIndex)
  {
    setKeys(keys);
    setAggregates(aggregates);
    this.schemaID = schemaID;
    this.aggregatorIndex = aggregatorIndex;

    initialize();
  }

  private void initialize()
  {
    eventKey = new EventKey(schemaID,
                            aggregatorIndex,
                            keys);
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

  public long getSchemaID()
  {
    return schemaID;
  }

  public EventKey getEventKey()
  {
    return eventKey;
  }

  public static class EventKey
  {
    private long schemaID;
    private int aggregatorIndex;
    private GPOMutable key;

    public EventKey(long schemaID,
                    int aggregatorIndex,
                    GPOMutable key)
    {
      setAggregatorIndex(aggregatorIndex);
      setKey(key);
    }

    /**
     * @return the aggregatorIndex
     */
    public int getAggregatorIndex()
    {
      return aggregatorIndex;
    }

    /**
     * @param aggregatorIndex the aggregatorIndex to set
     */
    private void setAggregatorIndex(int aggregatorIndex)
    {
      this.aggregatorIndex = aggregatorIndex;
    }

    /**
     * @return the schemaID
     */
    public long getSchemaID()
    {
      return schemaID;
    }

    /**
     * @param schemaID the schemaID to set
     */
    public void setSchemaID(long schemaID)
    {
      this.schemaID = schemaID;
    }

    /**
     * @return the key
     */
    public GPOMutable getKey()
    {
      return key;
    }

    /**
     * @param key the key to set
     */
    private void setKey(GPOMutable key)
    {
      Preconditions.checkNotNull(key);
      this.key = key;
    }
  }
}
