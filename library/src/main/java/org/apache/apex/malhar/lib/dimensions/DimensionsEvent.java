/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.apex.malhar.lib.dimensions;

import java.io.Serializable;
import java.util.List;

import org.apache.apex.malhar.lib.appdata.gpo.GPOMutable;
import org.apache.apex.malhar.lib.appdata.gpo.GPOUtils;
import org.apache.apex.malhar.lib.dimensions.aggregator.AggregateEvent;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;

/**
 * <p>
 * This is the base class for the events that are used for internal processing
 * in the subclasses of {@link AbstractDimensionsComputationFlexible} and
 * {@link DimensionsStoreHDHT}.
 * </p>
 * <p>
 * A {@link DimensionsEvent} is constructed from two parts: an {@link EventKey}
 * and a {@link GPOMutable} object which contains the values of aggregate
 * fields. The {@link EventKey} is used to identify the dimension combination an
 * event belongs to, and consequently determines what input values should be
 * aggregated together. The aggregates are the actual data payload of the event
 * which are to be aggregated.
 * </p>
 *
 * @since 3.1.0
 */
public class DimensionsEvent implements Serializable
{
  private static final long serialVersionUID = 201503231204L;

  /**
   * This is the {@link GPOMutable} object which holds all the aggregates.
   */
  protected GPOMutable aggregates;
  /**
   * This is the event key for the event.
   */
  protected EventKey eventKey;

  /**
   * Constructor for Kryo.
   */
  private DimensionsEvent()
  {
    //For kryo
  }

  /**
   * This creates a {@link DimensionsEvent} from the given event key and
   * aggregates.
   *
   * @param eventKey
   *          The key from which to create a {@link DimensionsEvent}.
   * @param aggregates
   *          The aggregates from which to create {@link DimensionsEvent}.
   */
  public DimensionsEvent(EventKey eventKey, GPOMutable aggregates)
  {
    setEventKey(eventKey);
    setAggregates(aggregates);
  }

  /**
   * Creates a DimensionsEvent with the given key values, aggregates and ids.
   *
   * @param keys
   *          The values for fields in the key.
   * @param aggregates
   *          The values for fields in the aggregate.
   * @param bucketID
   *          The bucketID
   * @param schemaID
   *          The schemaID.
   * @param dimensionDescriptorID
   *          The dimensionsDescriptorID.
   * @param aggregatorIndex
   *          The aggregatorIndex assigned to this event by the unifier.
   */
  public DimensionsEvent(GPOMutable keys, GPOMutable aggregates, int bucketID, int schemaID, int dimensionDescriptorID,
      int aggregatorIndex)
  {
    this.eventKey = new EventKey(bucketID, schemaID, dimensionDescriptorID, aggregatorIndex, keys);
    setAggregates(aggregates);
  }

  /**
   * This creates an event with the given data. Note, this constructor assumes
   * that the bucketID will be 0.
   *
   * @param keys
   *          The value for fields in the key.
   * @param aggregates
   *          The value for fields in the aggregate.
   * @param schemaID
   *          The schemaID.
   * @param dimensionDescriptorID
   *          The dimensionsDescriptorID.
   * @param aggregatorIndex
   *          The aggregatorIndex assigned to this event by the unifier.
   */
  public DimensionsEvent(GPOMutable keys, GPOMutable aggregates, int schemaID, int dimensionDescriptorID,
      int aggregatorIndex)
  {
    this.eventKey = new EventKey(schemaID, dimensionDescriptorID, aggregatorIndex, keys);
    setAggregates(aggregates);
  }

  /**
   * This is a helper method which sets the {@link EventKey} of the event to be
   * the same as the given {@link EventKey}.
   *
   * @param eventKey
   *          The {@link EventKey} to set on this event.
   */
  protected final void setEventKey(EventKey eventKey)
  {
    this.eventKey = new EventKey(eventKey);
  }

  /**
   * This is a helper method which sets the aggregates for this event.
   *
   * @param aggregates
   *          The aggregates for this event.
   */
  protected final void setAggregates(GPOMutable aggregates)
  {
    Preconditions.checkNotNull(aggregates);
    this.aggregates = aggregates;
  }

  /**
   * This is a helper method which returns the aggregates for this event.
   *
   * @return The helper method which returns the aggregates for this event.
   */
  public GPOMutable getAggregates()
  {
    return aggregates;
  }

  /**
   * Returns the {@link EventKey} for this event.
   *
   * @return The {@link EventKey} for this event.
   */
  public EventKey getEventKey()
  {
    return eventKey;
  }

  /**
   * This is a convenience method which returns the values of the key fields in
   * this event's {@link EventKey}.
   *
   * @return The values of the key fields in this event's {@link EventKey}.
   */
  public GPOMutable getKeys()
  {
    return eventKey.getKey();
  }

  /**
   * This is a convenience method which returns the schemaID of this event's
   * {@link EventKey}.
   *
   * @return The schemaID of this event's {@link EventKey}.
   */
  public int getSchemaID()
  {
    return eventKey.getSchemaID();
  }

  /**
   * Returns the id of the dimension descriptor (key combination) for which this
   * event contains data.
   *
   * @return The id of the dimension descriptor (key combination) for which this
   *         event contains data.
   */
  public int getDimensionDescriptorID()
  {
    return eventKey.getDimensionDescriptorID();
  }

  /**
   * Returns the id of the aggregator which is applied to this event's data.
   *
   * @return Returns the id of the aggregator which is applied to this event's
   *         data.
   */
  public int getAggregatorID()
  {
    return eventKey.getAggregatorID();
  }

  /**
   * Returns the bucketID assigned to this event. The bucketID is useful for
   * this event in the case that the event is sent to a partitioned HDHT
   * operator. Each partitioned HDHT operator can use the bucketIDs for the
   * buckets it writes to as a partition key.
   *
   * @return The bucketID assigned to this event.
   */
  public int getBucketID()
  {
    return eventKey.getBucketID();
  }

  /**
   * This is a utility method which copies the given src event to the given
   * destination event.
   *
   * @param aeDest
   *          The destination event.
   * @param aeSrc
   *          The source event.
   */
  public static void copy(DimensionsEvent aeDest, DimensionsEvent aeSrc)
  {
    GPOMutable destAggs = aeDest.getAggregates();
    GPOMutable srcAggs = aeSrc.getAggregates();

    if (srcAggs.getFieldsBoolean() != null) {
      System.arraycopy(srcAggs.getFieldsBoolean(), 0, destAggs.getFieldsBoolean(), 0,
          srcAggs.getFieldsBoolean().length);
    }

    if (srcAggs.getFieldsCharacter() != null) {
      System.arraycopy(srcAggs.getFieldsCharacter(), 0, destAggs.getFieldsCharacter(), 0,
          srcAggs.getFieldsCharacter().length);
    }

    if (srcAggs.getFieldsString() != null) {
      System.arraycopy(srcAggs.getFieldsString(), 0, destAggs.getFieldsString(), 0, srcAggs.getFieldsString().length);
    }

    if (srcAggs.getFieldsShort() != null) {
      System.arraycopy(srcAggs.getFieldsShort(), 0, destAggs.getFieldsShort(), 0, srcAggs.getFieldsShort().length);
    }

    if (srcAggs.getFieldsInteger() != null) {
      System.arraycopy(srcAggs.getFieldsInteger(), 0, destAggs.getFieldsInteger(), 0,
          srcAggs.getFieldsInteger().length);
    }

    if (srcAggs.getFieldsLong() != null) {
      System.arraycopy(srcAggs.getFieldsLong(), 0, destAggs.getFieldsLong(), 0, srcAggs.getFieldsLong().length);
    }

    if (srcAggs.getFieldsFloat() != null) {
      System.arraycopy(srcAggs.getFieldsFloat(), 0, destAggs.getFieldsFloat(), 0, srcAggs.getFieldsFloat().length);
    }

    if (srcAggs.getFieldsDouble() != null) {
      System.arraycopy(srcAggs.getFieldsDouble(), 0, destAggs.getFieldsDouble(), 0, srcAggs.getFieldsDouble().length);
    }
  }

  /**
   * <p>
   * The {@link EventKey} represents a dimensions combination for a dimensions
   * event. It contains the keys and values which define a dimensions
   * combination. It's very similar to a {@link DimensionsDescriptor} which is
   * also used to define part of a dimensions combination. The difference
   * between the two is that a {@link DimensionsDescriptor} only contains what
   * keys are included in the combination, not the values of those keys (which
   * the {@link EventKey} has.
   * </p>
   * <p>
   * In addition to holding the keys in a dimensions combination and their
   * values, the event key holds some meta information. The meta information
   * included and their purposes are the following:
   * <ul>
   * <li><b>bucketID:</b> This is set when the dimension store responsible for
   * storing the data is partitioned. In that case the bucketID is used as the
   * partitionID.</li>
   * <li><b>schemaID:</b> This is the id of the {@link DimensionalSchema} that
   * this {@link EventKey} corresponds to .</li>
   * <li><b>dimensionDescriptorID:</b> This is the id of the
   * {@link DimensionsDescriptor} that this {@link EventKey} corresponds to.
   * </li>
   * <li><b>aggregatorID:</b> This is the id of the aggregator that is used to
   * aggregate the values associated with this {@link EventKey} in a
   * {@link DimensionsEvent}.</li>
   * </ul>
   * </p>
   */
  public static class EventKey implements Serializable
  {
    private static final long serialVersionUID = 201503231205L;

    /**
     * The bucketID assigned to this event key.
     */
    private int bucketID;
    /**
     * The schemaID corresponding to the {@link DimensionalSchema} that this
     * {@link EventKey} corresponds to.
     */
    private int schemaID;
    /**
     * The dimensionsDescriptorID of the {@link DimensionDescriptor} in the
     * corresponding {@link DimensionalSchema}.
     */
    private int dimensionDescriptorID;
    /**
     * The id of the aggregator which should be used to aggregate the values
     * corresponding to this {@link EventKey}.
     */
    private int aggregatorID;
    /**
     * The values of the key fields.
     */
    private GPOMutable key;

    /**
     * Constructor for serialization.
     */
    private EventKey()
    {
      //For kryo
    }

    /**
     * Copy constructor.
     *
     * @param eventKey
     *          The {@link EventKey} whose data will be copied.
     */
    public EventKey(EventKey eventKey)
    {
      this.bucketID = eventKey.bucketID;
      this.schemaID = eventKey.schemaID;
      this.dimensionDescriptorID = eventKey.dimensionDescriptorID;
      this.aggregatorID = eventKey.aggregatorID;

      this.key = new GPOMutable(eventKey.getKey());
    }

    /**
     * Creates an event key with the given data.
     *
     * @param bucketID
     *          The bucketID assigned to this {@link EventKey}.
     * @param schemaID
     *          The schemaID of the corresponding {@link DimensionalSchema}.
     * @param dimensionDescriptorID
     *          The dimensionDescriptorID of the corresponding
     *          {@link DimensionDescriptor} in the {@link DimensionalSchema}.
     * @param aggregatorID
     *          The id of the aggregator which should be used to aggregate the
     *          values corresponding to this {@link EventKey}.
     * @param key
     *          The values of the keys.
     */
    public EventKey(int bucketID, int schemaID, int dimensionDescriptorID, int aggregatorID, GPOMutable key)
    {
      setBucketID(bucketID);
      setSchemaID(schemaID);
      setDimensionDescriptorID(dimensionDescriptorID);
      setAggregatorID(aggregatorID);
      setKey(key);
    }

    /**
     * Creates an event key with the given data. This constructor assumes that
     * the bucketID will be 0.
     *
     * @param schemaID
     *          The schemaID of the corresponding {@link DimensionalSchema}.
     * @param dimensionDescriptorID
     *          The dimensionDescriptorID of the corresponding
     *          {@link DimensionDescriptor}.
     * @param aggregatorID
     *          The id of the aggregator which should be used to aggregate the
     *          values corresponding to this {@link EventKey}.
     * @param key
     *          The values of the keys.
     */
    public EventKey(int schemaID, int dimensionDescriptorID, int aggregatorID, GPOMutable key)
    {
      setSchemaID(schemaID);
      setDimensionDescriptorID(dimensionDescriptorID);
      setAggregatorID(aggregatorID);
      setKey(key);
    }

    /**
     * Sets the dimension descriptor ID.
     *
     * @param dimensionDescriptorID
     *          The dimension descriptor ID to set.
     */
    private void setDimensionDescriptorID(int dimensionDescriptorID)
    {
      this.dimensionDescriptorID = dimensionDescriptorID;
    }

    /**
     * Returns the dimension descriptor ID.
     *
     * @return The dimension descriptor ID.
     */
    public int getDimensionDescriptorID()
    {
      return dimensionDescriptorID;
    }

    /**
     * Returns the aggregatorID.
     *
     * @return The aggregatorID.
     */
    public int getAggregatorID()
    {
      return aggregatorID;
    }

    /**
     * Sets the aggregatorID.
     *
     * @param aggregatorID
     *          The aggregatorID to set.
     */
    private void setAggregatorID(int aggregatorID)
    {
      this.aggregatorID = aggregatorID;
    }

    /**
     * Returns the schemaID.
     *
     * @return The schemaID to set.
     */
    public int getSchemaID()
    {
      return schemaID;
    }

    /**
     * Sets the schemaID.
     *
     * @param schemaID
     *          The schemaID to set.
     */
    private void setSchemaID(int schemaID)
    {
      this.schemaID = schemaID;
    }

    /**
     * Returns the key values.
     *
     * @return The key values.
     */
    public GPOMutable getKey()
    {
      return key;
    }

    /**
     * Sets the bucektID.
     *
     * @param bucketID
     *          The bucketID.
     */
    private void setBucketID(int bucketID)
    {
      this.bucketID = bucketID;
    }

    /**
     * Gets the bucketID.
     *
     * @return The bucketID.
     */
    public int getBucketID()
    {
      return bucketID;
    }

    /**
     * Sets the key values.
     *
     * @param key
     *          The key values to set.
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
      if (obj == null) {
        return false;
      }

      if (getClass() != obj.getClass()) {
        return false;
      }

      final EventKey other = (EventKey)obj;

      if (this.bucketID != other.bucketID) {
        return false;
      }

      if (this.schemaID != other.schemaID) {
        return false;
      }

      if (this.dimensionDescriptorID != other.dimensionDescriptorID) {
        return false;
      }

      if (this.aggregatorID != other.aggregatorID) {
        return false;
      }

      if (this.key != other.key && (this.key == null || !this.key.equals(other.key))) {
        return false;
      }

      return true;
    }

    @Override
    public String toString()
    {
      return "EventKey{" + "schemaID=" + schemaID + ", dimensionDescriptorID=" + dimensionDescriptorID
          + ", aggregatorIndex=" + aggregatorID + ", key=" + key + '}';
    }

    public static List<EventKey> createEventKeys(int schemaId, int dimensionsDescriptorId, int aggregatorId,
        List<GPOMutable> keys)
    {
      List<EventKey> eventKeys = Lists.newArrayList();

      for (GPOMutable key : keys) {
        eventKeys.add(new EventKey(schemaId, dimensionsDescriptorId, aggregatorId, key));
      }

      return eventKeys;
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
    if (obj == null) {
      return false;
    }
    if (getClass() != obj.getClass()) {
      return false;
    }
    final DimensionsEvent other = (DimensionsEvent)obj;
    if (this.aggregates != other.aggregates && (this.aggregates == null || !this.aggregates.equals(other.aggregates))) {
      return false;
    }
    if (this.eventKey != other.eventKey && (this.eventKey == null || !this.eventKey.equals(other.eventKey))) {
      return false;
    }
    return true;
  }

  public static class InputEvent extends DimensionsEvent
  {
    private static final long serialVersionUID = 201506210406L;
    public boolean used = false;

    private InputEvent()
    {
    }

    /**
     * This creates a {@link DimensionsEvent} from the given event key and
     * aggregates.
     *
     * @param eventKey
     *          The key from which to create a {@link DimensionsEvent}.
     * @param aggregates
     *          The aggregates from which to create {@link DimensionsEvent}.
     */
    public InputEvent(EventKey eventKey, GPOMutable aggregates)
    {
      setEventKey(eventKey);
      setAggregates(aggregates);
    }

    /**
     * Creates a DimensionsEvent with the given key values, aggregates and ids.
     *
     * @param keys
     *          The values for fields in the key.
     * @param aggregates
     *          The values for fields in the aggregate.
     * @param bucketID
     *          The bucketID
     * @param schemaID
     *          The schemaID.
     * @param dimensionDescriptorID
     *          The dimensionsDescriptorID.
     * @param aggregatorIndex
     *          The aggregatorIndex assigned to this event by the unifier.
     */
    public InputEvent(GPOMutable keys, GPOMutable aggregates, int bucketID, int schemaID, int dimensionDescriptorID,
        int aggregatorIndex)
    {
      this.eventKey = new EventKey(bucketID, schemaID, dimensionDescriptorID, aggregatorIndex, keys);
      setAggregates(aggregates);
    }

    /**
     * This creates an event with the given data. Note, this constructor assumes
     * that the bucketID will be 0.
     *
     * @param keys
     *          The value for fields in the key.
     * @param aggregates
     *          The value for fields in the aggregate.
     * @param schemaID
     *          The schemaID.
     * @param dimensionDescriptorID
     *          The dimensionsDescriptorID.
     * @param aggregatorIndex
     *          The aggregatorIndex assigned to this event by the unifier.
     */
    public InputEvent(GPOMutable keys, GPOMutable aggregates, int schemaID, int dimensionDescriptorID,
        int aggregatorIndex)
    {
      this.eventKey = new EventKey(schemaID, dimensionDescriptorID, aggregatorIndex, keys);
      setAggregates(aggregates);
    }

    @Override
    public int hashCode()
    {
      return GPOUtils.hashcode(this.getKeys());
    }

    @Override
    public boolean equals(Object obj)
    {
      if (obj == null) {
        return false;
      }
      if (getClass() != obj.getClass()) {
        return false;
      }
      final DimensionsEvent other = (DimensionsEvent)obj;

      if (this.eventKey != other.eventKey && (this.eventKey == null || !this.eventKey.equals(other.eventKey))) {
        return false;
      }
      return true;
    }
  }

  public static class Aggregate extends DimensionsEvent implements AggregateEvent
  {
    private static final long serialVersionUID = 201506190110L;

    /**
     * This is the aggregatorIndex assigned to this event.
     */
    protected int aggregatorIndex;
    private GPOMutable metaData;

    public Aggregate()
    {
      //for kryo and for extending classes
    }

    /**
     * This creates a {@link DimensionsEvent} from the given event key and
     * aggregates.
     *
     * @param eventKey
     *          The key from which to create a {@link DimensionsEvent}.
     * @param aggregates
     *          The aggregates from which to create {@link DimensionsEvent}.
     */
    public Aggregate(EventKey eventKey, GPOMutable aggregates)
    {
      setEventKey(eventKey);
      setAggregates(aggregates);
    }

    public Aggregate(EventKey eventKey, GPOMutable aggregates, GPOMutable metaData)
    {
      super(eventKey, aggregates);

      this.metaData = metaData;
    }

    /**
     * Creates a DimensionsEvent with the given key values, aggregates and ids.
     *
     * @param keys
     *          The values for fields in the key.
     * @param aggregates
     *          The values for fields in the aggregate.
     * @param bucketID
     *          The bucketID
     * @param schemaID
     *          The schemaID.
     * @param dimensionDescriptorID
     *          The dimensionsDescriptorID.
     * @param aggregatorIndex
     *          The aggregatorIndex assigned to this event by the unifier.
     */
    public Aggregate(GPOMutable keys, GPOMutable aggregates, int bucketID, int schemaID, int dimensionDescriptorID,
        int aggregatorIndex)
    {
      this.eventKey = new EventKey(bucketID, schemaID, dimensionDescriptorID, aggregatorIndex, keys);
      setAggregates(aggregates);
    }

    public Aggregate(GPOMutable keys, GPOMutable aggregates, GPOMutable metaData, int bucketID, int schemaID,
        int dimensionDescriptorID, int aggregatorIndex)
    {
      this(keys, aggregates, bucketID, schemaID, dimensionDescriptorID, aggregatorIndex);

      this.metaData = metaData;
    }

    /**
     * This creates an event with the given data. Note, this constructor assumes
     * that the bucketID will be 0.
     *
     * @param keys
     *          The value for fields in the key.
     * @param aggregates
     *          The value for fields in the aggregate.
     * @param schemaID
     *          The schemaID.
     * @param dimensionDescriptorID
     *          The dimensionsDescriptorID.
     * @param aggregatorIndex
     *          The aggregatorIndex assigned to this event by the unifier.
     */
    public Aggregate(GPOMutable keys, GPOMutable aggregates, int schemaID, int dimensionDescriptorID,
        int aggregatorIndex)
    {
      this.eventKey = new EventKey(schemaID, dimensionDescriptorID, aggregatorIndex, keys);
      setAggregates(aggregates);
    }

    public Aggregate(GPOMutable keys, GPOMutable aggregates, GPOMutable metaData, int schemaID,
        int dimensionDescriptorID, int aggregatorIndex)
    {
      this(keys, aggregates, schemaID, dimensionDescriptorID, aggregatorIndex);

      this.metaData = metaData;
    }

    public void setMetaData(GPOMutable metaData)
    {
      this.metaData = metaData;
    }

    public GPOMutable getMetaData()
    {
      return metaData;
    }

    public void setAggregatorIndex(int aggregatorIndex)
    {
      this.aggregatorIndex = aggregatorIndex;
    }

    @Override
    public int getAggregatorIndex()
    {
      return aggregatorIndex;
    }

    @Override
    public int hashCode()
    {
      return GPOUtils.hashcode(this.getKeys());
    }

    @Override
    public boolean equals(Object obj)
    {
      if (obj == null) {
        return false;
      }
      if (getClass() != obj.getClass()) {
        return false;
      }
      final DimensionsEvent other = (DimensionsEvent)obj;

      if (this.eventKey != other.eventKey && (this.eventKey == null || !this.eventKey.equals(other.eventKey))) {
        return false;
      }
      return true;
    }
  }
}
