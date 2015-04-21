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

import com.datatorrent.lib.appdata.gpo.GPOImmutable;
import com.datatorrent.lib.appdata.gpo.GPOMutable;
import com.datatorrent.lib.statistics.DimensionsComputation;
import com.google.common.base.Preconditions;
import java.io.Serializable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AggregateEvent implements DimensionsComputation.AggregateEvent, Serializable
{
  private static final Logger logger = LoggerFactory.getLogger(AggregateEvent.class);
  private static final long serialVersionUID = 201503231204L;

  private GPOMutable aggregates;
  private EventKey eventKey;

  private boolean empty = false;

  public AggregateEvent()
  {
    empty = true;
  }

  public AggregateEvent(EventKey eventKey,
                        GPOMutable aggregates)
  {
    setEventKey(eventKey);
    setAggregates(aggregates);
  }

  public AggregateEvent(GPOImmutable keys,
                        GPOMutable aggregates,
                        int bucketID,
                        int schemaID,
                        int dimensionDescriptorID,
                        int aggregatorIndex)
  {
    this.eventKey = new EventKey(bucketID,
                                 schemaID,
                                 dimensionDescriptorID,
                                 aggregatorIndex,
                                 keys);
    setAggregates(aggregates);
  }

  public AggregateEvent(GPOImmutable keys,
                        GPOMutable aggregates,
                        int schemaID,
                        int dimensionDescriptorID,
                        int aggregatorIndex)
  {
    this.eventKey = new EventKey(schemaID,
                                 dimensionDescriptorID,
                                 aggregatorIndex,
                                 keys);
    setAggregates(aggregates);
  }

  private void setEventKey(EventKey eventKey)
  {
    this.eventKey = new EventKey(eventKey);
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

  public EventKey getEventKey()
  {
    return eventKey;
  }

  public GPOMutable getKeys()
  {
    return eventKey.getKey();
  }

  public int getSchemaID()
  {
    return eventKey.getSchemaID();
  }

  public int getDimensionDescriptorID()
  {
    return eventKey.getDimensionDescriptorID();
  }

  @Override
  public int getAggregatorIndex()
  {
    return eventKey.getAggregatorIndex();
  }

  public int getBucketID()
  {
    return eventKey.getBucketID();
  }

  /**
   * @return the empty
   */
  public boolean isEmpty()
  {
    return empty;
  }

  public static class EventKey implements Serializable
  {
    private static final long serialVersionUID = 201503231205L;

    private int bucketID;
    private int schemaID;
    private int dimensionDescriptorID;
    private int aggregatorIndex;
    private GPOMutable key;

    public EventKey()
    {
    }

    public EventKey(EventKey eventKey)
    {
      this.bucketID = eventKey.bucketID;
      this.schemaID = eventKey.schemaID;
      this.dimensionDescriptorID = eventKey.dimensionDescriptorID;
      this.aggregatorIndex = eventKey.aggregatorIndex;

      this.key = eventKey.getKey();
    }

    public EventKey(int bucketID,
                    int schemaID,
                    int dimensionDescriptorID,
                    int aggregatorIndex,
                    GPOMutable key)
    {
      setBucketID(bucketID);
      setSchemaID(schemaID);
      setDimensionDescriptorID(dimensionDescriptorID);
      setAggregatorIndex(aggregatorIndex);
      setKey(key);
    }

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

    private void setBucketID(int bucketID)
    {
      this.bucketID = bucketID;
    }

    public int getBucketID()
    {
      return bucketID;
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
      hash = 97 * hash + this.bucketID;
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

      if(this.bucketID != other.bucketID) {
        return false;
      }

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

    @Override
    public String toString()
    {
      return "EventKey{" + "schemaID=" + schemaID + ", dimensionDescriptorID=" + dimensionDescriptorID + ", aggregatorIndex=" + aggregatorIndex + ", key=" + key + '}';
    }
  }
}
