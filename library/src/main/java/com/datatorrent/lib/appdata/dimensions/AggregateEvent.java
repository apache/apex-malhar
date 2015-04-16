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
import com.google.common.primitives.Longs;
import java.io.Serializable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AggregateEvent implements DimensionsComputation.AggregateEvent, Serializable
{
  private static final Logger logger = LoggerFactory.getLogger(AggregateEvent.class);
  private static final long serialVersionUID = 201503231204L;

  private int schemaID;
  private int dimensionDescriptorID;
  private int aggregatorIndex;

  private GPOImmutable keys;
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
    Preconditions.checkNotNull(eventKey);

    this.keys = (GPOImmutable) eventKey.getKey();
    this.schemaID = eventKey.getSchemaID();
    this.dimensionDescriptorID = eventKey.getDimensionDescriptorID();
    this.aggregatorIndex = eventKey.getAggregatorIndex();

    this.setAggregates(aggregates);

    initialize();
  }

  public AggregateEvent(GPOImmutable keys,
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

    private int schemaID;
    private int dimensionDescriptorID;
    private int aggregatorIndex;
    private GPOMutable key;

    public EventKey()
    {
    }

    public EventKey(EventKey eventKey)
    {
      this.schemaID = eventKey.schemaID;
      this.dimensionDescriptorID = eventKey.dimensionDescriptorID;
      this.aggregatorIndex = eventKey.aggregatorIndex;

      this.key = new GPOMutable(eventKey.getKey());
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

    public boolean logEquals = false;

    @Override
    public boolean equals(Object obj)
    {
      int counter = 0;

      if(logEquals) {
      counter++;
      logger.info("Got here {}", counter);
      }

      if(obj == null) {
        return false;
      }

      if(logEquals) {
      counter++;
      logger.info("Got here {}", counter);
      }

      if(getClass() != obj.getClass()) {
        return false;
      }

      if(logEquals) {
      counter++;
      logger.info("Got here {}", counter);
      }

      final EventKey other = (EventKey)obj;
      if(this.schemaID != other.schemaID) {
        return false;
      }

      if(logEquals) {
      counter++;
      logger.info("Got here {}", counter);
      }

      if(this.dimensionDescriptorID != other.dimensionDescriptorID) {
        return false;
      }

      if(logEquals) {
      counter++;
      logger.info("Got here {}", counter);
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

  public static class ID implements Comparable<ID>
  {
    private long msb;
    private long mmsb;
    private long lsb;

    public ID(long msb, long mmsb, long lsb)
    {
      if(msb < 0L ||
         mmsb < 0L ||
         lsb < 0L) {
        throw new IllegalArgumentException("msb, mmsb, lsb must be nonnegative:\n" +
                                           "msb:" + msb + "\n" +
                                           "mmsb:" + mmsb + "\n" +
                                           "lsb:" + lsb + "\n");

      }

      this.msb = msb;
      this.mmsb = mmsb;
      this.lsb = lsb;
    }

    public long getMSB()
    {
      return msb;
    }

    public long getMMSB()
    {
      return mmsb;
    }

    public long getLSB()
    {
      return lsb;
    }

    public byte[] getBytes()
    {
      byte[] idBytes = new byte[24];

      byte[] msbs = Longs.toByteArray(msb);
      byte[] mmsbs = Longs.toByteArray(mmsb);
      byte[] lsbs = Longs.toByteArray(lsb);

      int index = 0;

      for(int tindex = 0; tindex < 8; tindex++, index++) {
        idBytes[index] = msbs[tindex];
      }

      for(int tindex = 0; tindex < 8; tindex++, index++) {
        idBytes[index] = mmsbs[tindex];
      }

      for(int tindex = 0; tindex < 8; tindex++, index++) {
        idBytes[index] = lsbs[tindex];
      }

      return idBytes;
    }

    @Override
    public int compareTo(ID id)
    {
      if(msb < id.msb) {
        return -1;
      }
      else if(msb > id.msb) {
        return 1;
      }

      if(mmsb < id.mmsb) {
        return -1;
      }
      else if(mmsb > id.mmsb) {
        return 1;
      }

      if(lsb < id.lsb) {
        return -1;
      }
      else if(lsb > id.lsb) {
        return 1;
      }

      return 0;
    }
  }

  public static class IDGenerator
  {
    long msb = 0;
    long mmsb = 0;
    long lsb = 0;

    public IDGenerator()
    {
    }

    public synchronized ID getNextID()
    {
      if(lsb == Long.MAX_VALUE) {
        if(mmsb == Long.MAX_VALUE) {
          if(msb == Long.MAX_VALUE) {
            throw new UnsupportedOperationException("Max ID reached! This is like 1000 years from now!");
          }
          else {
            msb++;
          }
        }
        else {
          mmsb++;
        }
      }
      else {
        lsb++;
      }

      return new ID(msb, mmsb, lsb);
    }
  }
}
