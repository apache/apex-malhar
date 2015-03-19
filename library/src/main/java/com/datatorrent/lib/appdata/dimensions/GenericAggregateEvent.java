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
  private int schemaID;
  private int dimensionDescriptorID;
  private int aggregatorIndex;

  private GPOImmutable keys;
  private GPOMutable aggregates;
  private EventKey eventKey;

  public GenericAggregateEvent(GPOImmutable keys,
                               GPOMutable aggregates,
                               int schemaID,
                               int dimensionDescriptorID,
                               int aggregatorIndex)
  {
    setKeys(keys);
    setAggregates(aggregates);
    this.dimensionDescriptorID = dimensionDescriptorID;
    this.schemaID = schemaID;
    this.aggregatorIndex = aggregatorIndex;

    initialize();
  }

  private void initialize()
  {
    eventKey = new EventKey(schemaID,
                            dimensionDescriptorID,
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

  public int getSchemaID()
  {
    return schemaID;
  }

  public int getDimensionDescriptorID()
  {
    return dimensionDescriptorID;
  }

  @Override
  public int getAggregatorIndex()
  {
    return aggregatorIndex;
  }

  public EventKey getEventKey()
  {
    return eventKey;
  }

  public static class EventKey
  {
    private int schemaID;
    private int dimensionDescriptorID;
    private int aggregatorIndex;
    private GPOMutable key;

    public EventKey(int schemaID,
                    int dimensionDescriptorID,
                    int aggregatorIndex,
                    GPOMutable key)
    {
      setSchemaID(schemaID);
      setDimensionDescriptorID(dimensionDescriptorID);
      setAggregatorIndex(aggregatorIndex);
      setKey(key);
    }

    private void setDimensionDescriptorID(int dimensionDescriptorID)
    {
      this.dimensionDescriptorID = dimensionDescriptorID;
    }

    public int getDimensionDescriptorID()
    {
      return dimensionDescriptorID;
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
    public int getSchemaID()
    {
      return schemaID;
    }

    /**
     * @param schemaID the schemaID to set
     */
    private void setSchemaID(int schemaID)
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
    
    @Override
    public int hashCode()
    {
      int hash = 3;
      hash = 97 * hash + this.schemaID;
      hash = 97 * hash + this.dimensionDescriptorID;
      hash = 97 * hash + this.aggregatorIndex;
      hash = 97 * hash + (this.key != null ? this.key.hashCode() : 0);
      return hash;
    }

    @Override
    public boolean equals(Object obj)
    {
      if(obj == null) {
        return false;
      }
      if(getClass() != obj.getClass()) {
        return false;
      }
      final EventKey other = (EventKey)obj;
      if(this.schemaID != other.schemaID) {
        return false;
      }
      if(this.dimensionDescriptorID != other.dimensionDescriptorID) {
        return false;
      }
      if(this.aggregatorIndex != other.aggregatorIndex) {
        return false;
      }
      if(this.key != other.key && (this.key == null || !this.key.equals(other.key))) {
        return false;
      }
      return true;
    }
  }
}
