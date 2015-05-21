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
package com.datatorrent.lib.dimensions;

import com.datatorrent.lib.dimensions.AbstractDimensionsComputation.UnifiableAggregate;
import com.datatorrent.lib.appdata.gpo.GPOMutable;
import com.google.common.base.Preconditions;
import java.io.Serializable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class DimensionsEvent implements Serializable
{
  private static final Logger logger = LoggerFactory.getLogger(DimensionsEvent.class);
  private static final long serialVersionUID = 201503231204L;

  private GPOMutable aggregates;
  private EventKey eventKey;

  private DimensionsEvent()
  {
    //For kryo
  }

  public DimensionsEvent(EventKey eventKey,
                        GPOMutable aggregates)
  {
    setEventKey(eventKey);
    setAggregates(aggregates);
  }

  public DimensionsEvent(GPOMutable keys,
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

  public DimensionsEvent(GPOMutable keys,
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

  public int getAggregatorID()
  {
    return eventKey.getAggregatorID();
  }

  public int getBucketID()
  {
    return eventKey.getBucketID();
  }

  public static void copy(DimensionsEvent aeDest, DimensionsEvent aeSrc)
  {
    GPOMutable destAggs = aeDest.getAggregates();
    GPOMutable srcAggs = aeSrc.getAggregates();

    if(srcAggs.getFieldsBoolean() != null) {
      System.arraycopy(srcAggs.getFieldsBoolean(), 0, destAggs.getFieldsBoolean(), 0, srcAggs.getFieldsBoolean().length);
    }

    if(srcAggs.getFieldsCharacter() != null) {
      System.arraycopy(srcAggs.getFieldsCharacter(), 0, destAggs.getFieldsCharacter(), 0, srcAggs.getFieldsCharacter().length);
    }

    if(srcAggs.getFieldsString() != null) {
      System.arraycopy(srcAggs.getFieldsString(), 0, destAggs.getFieldsString(), 0, srcAggs.getFieldsString().length);
    }

    if(srcAggs.getFieldsShort() != null) {
      System.arraycopy(srcAggs.getFieldsShort(), 0, destAggs.getFieldsShort(), 0, srcAggs.getFieldsShort().length);
    }

    if(srcAggs.getFieldsInteger() != null) {
      System.arraycopy(srcAggs.getFieldsInteger(), 0, destAggs.getFieldsInteger(), 0, srcAggs.getFieldsInteger().length);
    }

    if(srcAggs.getFieldsLong() != null) {
      System.arraycopy(srcAggs.getFieldsLong(), 0, destAggs.getFieldsLong(), 0, srcAggs.getFieldsLong().length);
    }

    if(srcAggs.getFieldsFloat() != null) {
      System.arraycopy(srcAggs.getFieldsFloat(), 0, destAggs.getFieldsFloat(), 0, srcAggs.getFieldsFloat().length);
    }

    if(srcAggs.getFieldsDouble() != null) {
      System.arraycopy(srcAggs.getFieldsDouble(), 0, destAggs.getFieldsDouble(), 0, srcAggs.getFieldsDouble().length);
    }
  }

  public static class EventKey implements Serializable
  {
    private static final long serialVersionUID = 201503231205L;

    private int bucketID;
    private int schemaID;
    private int dimensionDescriptorID;
    private int aggregatorID;
    private GPOMutable key;

    public EventKey()
    {
    }

    public EventKey(EventKey eventKey)
    {
      this.bucketID = eventKey.bucketID;
      this.schemaID = eventKey.schemaID;
      this.dimensionDescriptorID = eventKey.dimensionDescriptorID;
      this.aggregatorID = eventKey.aggregatorID;

      this.key = new GPOMutable(eventKey.getKey());
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
      setAggregatorID(aggregatorIndex);
      setKey(key);
    }

    public EventKey(int schemaID,
                    int dimensionDescriptorID,
                    int aggregatorIndex,
                    GPOMutable key)
    {
      setSchemaID(schemaID);
      setDimensionDescriptorID(dimensionDescriptorID);
      setAggregatorID(aggregatorIndex);
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
     * @return the aggregatorID
     */
    public int getAggregatorID()
    {
      return aggregatorID;
    }

    /**
     * @param aggregatorID the aggregatorID to set
     */
    private void setAggregatorID(int aggregatorID)
    {
      this.aggregatorID = aggregatorID;
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
      hash = 97 * hash + this.aggregatorID;
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

      if(this.aggregatorID != other.aggregatorID) {
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
      return "EventKey{" + "schemaID=" + schemaID + ", dimensionDescriptorID=" + dimensionDescriptorID + ", aggregatorIndex=" + aggregatorID + ", key=" + key + '}';
    }
  }

  @Override
  public int hashCode()
  {
    int hash = 5;
    hash = 79 * hash + (this.aggregates != null ? this.aggregates.hashCode() : 0);
    hash = 79 * hash + (this.eventKey != null ? this.eventKey.hashCode() : 0);
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
    final DimensionsEvent other = (DimensionsEvent)obj;
    if(this.aggregates != other.aggregates && (this.aggregates == null || !this.aggregates.equals(other.aggregates))) {
      return false;
    }
    if(this.eventKey != other.eventKey && (this.eventKey == null || !this.eventKey.equals(other.eventKey))) {
      return false;
    }
    return true;
  }

  public static class InputEvent extends DimensionsEvent
  {
    private static final long serialVersionUID = 201505181028L;

    private InputEvent()
    {
      //For kryo
    }

    public InputEvent(EventKey eventKey,
                               GPOMutable aggregates)
    {
      super(eventKey,
            aggregates);
    }

    public InputEvent(GPOMutable keys,
                      GPOMutable aggregates,
                      int bucketID,
                      int schemaID,
                      int dimensionDescriptorID,
                      int aggregatorIndex)
    {
      super(keys,
            aggregates,
            bucketID,
            schemaID,
            dimensionDescriptorID,
            aggregatorIndex);
    }

    public InputEvent(GPOMutable keys,
                      GPOMutable aggregates,
                      int schemaID,
                      int dimensionDescriptorID,
                      int aggregatorIndex)
    {
      super(keys,
            aggregates,
            schemaID,
            dimensionDescriptorID,
            aggregatorIndex);
    }
  }

  public static class Aggregate extends DimensionsEvent implements UnifiableAggregate
  {
    private static final long serialVersionUID = 201505181028L;

    private int aggregateIndex;

    private Aggregate()
    {
      //For kryo
    }

    public Aggregate(EventKey eventKey,
                     GPOMutable aggregates)
    {
      super(eventKey,
            aggregates);
    }

    public Aggregate(GPOMutable keys,
                     GPOMutable aggregates,
                     int bucketID,
                     int schemaID,
                     int dimensionDescriptorID,
                     int aggregatorIndex)
    {
      super(keys,
            aggregates,
            bucketID,
            schemaID,
            dimensionDescriptorID,
            aggregatorIndex);
    }

    public Aggregate(GPOMutable keys,
                     GPOMutable aggregates,
                     int schemaID,
                     int dimensionDescriptorID,
                     int aggregatorIndex)
    {
      super(keys,
            aggregates,
            schemaID,
            dimensionDescriptorID,
            aggregatorIndex);
    }

    @Override
    public int getAggregateIndex()
    {
      return aggregateIndex;
    }

    @Override
    public void setAggregateIndex(int aggregateIndex)
    {
      this.aggregateIndex = aggregateIndex;
    }
  }
}
