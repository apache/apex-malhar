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

import com.datatorrent.lib.appdata.gpo.GPOMutable;
import com.datatorrent.lib.dimensions.AbstractDimensionsComputation.DTHashingStrategy;
import com.datatorrent.lib.dimensions.AbstractDimensionsComputation.DimensionsCombination;
import com.datatorrent.lib.dimensions.DimensionsComputation.UnifiableAggregate;
import com.google.common.base.Preconditions;
import java.io.Serializable;

/**
 * <p>
 * This is the base class for the events that are used for internal processing in the subclasses of
 * {@link AbstractDimensionsComputationFlexible} and {@link DimensionsStoreHDHT}.
 * </p>
 * <p>
 * A {@link DimensionsEvent} is constructed from two parts: an {@link EventKey} and a {@link GPOMutable} object
 * which contains the values of aggregate fields. The {@link EventKey} is used to identify the dimension combination
 * an event belongs to, and consequently determines what input values should be aggregated together. The aggregates
 * are the actual data payload of the event which are to be aggregated.
 * </p>
 */
public abstract class DimensionsEvent implements Serializable, UnifiableAggregate
{
  private static final long serialVersionUID = 201503231204L;

  /**
   * This is the {@link GPOMutable} object which holds all the aggregates.
   */
  private GPOMutable aggregates;
  /**
   * This is the event key for the event.
   */
  private EventKey eventKey;
  /**
   * This is the aggregateIndex assigned to this event.
   */
  private int aggregateIndex;

  /**
   * Constructor for Kryo.
   */
  private DimensionsEvent()
  {
    //For kryo
  }

  /**
   * This creates a {@link DimensionsEvent} from the given event key and aggregates.
   * @param eventKey The key from which to create a {@link DimensionsEvent}.
   * @param aggregates The aggregates from which to create {@link DimensionsEvent}.
   */
  public DimensionsEvent(EventKey eventKey,
                         GPOMutable aggregates)
  {
    setEventKey(eventKey);
    setAggregates(aggregates);
  }

  /**
   * Creates a DimensionsEvent with the given key values, aggregates and ids.
   * @param keys The values for fields in the key.
   * @param aggregates The values for fields in the aggregate.
   * @param bucketID The bucketID
   * @param schemaID The schemaID.
   * @param dimensionDescriptorID The dimensionsDescriptorID.
   * @param aggregatorIndex The aggregateIndex assigned to this event by the unifier.
   */
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

  /**
   * This creates an event with the given data. Note, this constructor assumes that the bucketID will be 0.
   * @param keys The value for fields in the key.
   * @param aggregates The value for fields in the aggregate.
   * @param schemaID The schemaID.
   * @param dimensionDescriptorID The dimensionsDescriptorID.
   * @param aggregatorIndex  The aggregateIndex assigned to this event by the unifier.
   */
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

  /**
   * This is a helper method which sets the {@link EventKey} of the event to
   * be the same as the given {@link EventKey}.
   * @param eventKey The {@link EventKey} to set on this event.
   */
  private void setEventKey(EventKey eventKey)
  {
    this.eventKey = new EventKey(eventKey);
  }

  /**
   * This is a helper method which sets the aggregates for this event.
   * @param aggregates The aggregates for this event.
   */
  private void setAggregates(GPOMutable aggregates)
  {
    Preconditions.checkNotNull(aggregates);
    this.aggregates = aggregates;
  }

  /**
   * This is a helper method which returns the aggregates for this event.
   * @return The helper method which returns the aggregates for this event.
   */
  public GPOMutable getAggregates()
  {
    return aggregates;
  }

  /**
   * Returns the {@link EventKey} for this event.
   * @return The {@link EventKey} for this event.
   */
  public EventKey getEventKey()
  {
    return eventKey;
  }

  /**
   * This is a convenience method which returns the values of the key fields in this event's
   * {@link EventKey}.
   * @return The values of the key fields in this event's {@link EventKey}.
   */
  public GPOMutable getKeys()
  {
    return eventKey.getKey();
  }

  /**
   * This is a convenience method which returns the schemaID of this event's {@link EventKey}.
   * @return The schemaID of this event's {@link EventKey}.
   */
  public int getSchemaID()
  {
    return eventKey.getSchemaID();
  }

  /**
   * Returns the id of the dimension descriptor (key combination) for which this event contains data.
   * @return The id of the dimension descriptor (key combination) for which this event contains data.
   */
  public int getDimensionDescriptorID()
  {
    return eventKey.getDimensionDescriptorID();
  }

  /**
   * Returns the id of the aggregator which is applied to this event's data.
   * @return Returns the id of the aggregator which is applied to this event's data.
   */
  public int getAggregatorID()
  {
    return eventKey.getAggregatorID();
  }

  /**
   * Returns the bucketID assigned to this event. The bucketID is useful for this event in the case that the event
   * is sent to a partitioned HDHT operator. Each partitioned HDHT operator can use the bucketIDs for the buckets it
   * writes to as a partition key.
   * @return The bucketID assigned to this event.
   */
  public int getBucketID()
  {
    return eventKey.getBucketID();
  }

  @Override
  public void setAggregateIndex(int aggregatorIndex)
  {
    this.aggregateIndex = aggregatorIndex;
  }

  @Override
  public int getAggregateIndex()
  {
    return aggregateIndex;
  }

  /**
   * This is a utility method which copies the given src event to the given destination event.
   * @param aeDest The destination event.
   * @param aeSrc The source event.
   */
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

  /**
   * <p>
   * The {@link EventKey} represents a dimensions combination for a dimensions event. It contains the keys and values
   * which define a dimensions combination. It's very similar to a {@link DimensionsDescriptor} which is also used to
   * define part of a dimensions combination. The difference between the two is that a {@link DimensionsDescriptor} only contains what
   * keys are included in the combination, not the values of those keys (which the {@link EventKey} has.
   * </p>
   * <p>
   * In addition to holding the keys in a dimensions combination and their values, the event key holds some meta information.
   * The meta information included and their purposes are the following:
   * <ul>
   *  <li><b>bucketID:</b> This is set when the dimension store responsible for storing the data is partitioned. In that
   *  case the bucketID is used as the partitionID.</li>
   *  <li><b>schemaID:</b> This is the id of the {@link DimensionalSchema} that this {@link EventKey} corresponds to.</li>
   *  <li><b>dimensionDescriptorID:</b> This is the id of the {@link DimensionsDescriptor} that this {@link EventKey} corresponds to.</li>
   *  <li><b>aggregatorID:</b> This is the id of the aggregator that is used to aggregate the values associated with this {@link EventKey}
   *  in a {@link DimensionsEvent}.</li>
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
     * The schemaID corresponding to the {@link DimensionalSchema} that this {@link EventKey}
     * corresponds to.
     */
    private int schemaID;
    /**
     * The dimensionsDescriptorID of the {@link DimensionDescriptor} in the corresponding {@link DimensionalSchema}.
     */
    private int dimensionDescriptorID;
    /**
     * The id of the aggregator which should be used to aggregate the values corresponding to this
     * {@link EventKey}.
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
     * @param eventKey The {@link EventKey} whose data will be copied.
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
     * @param bucketID The bucketID assigned to this {@link EventKey}.
     * @param schemaID The schemaID of the corresponding {@link DimensionalSchema}.
     * @param dimensionDescriptorID The dimensionDescriptorID of the corresponding {@link DimensionDescriptor} in the {@link DimensionalSchema}.
     * @param aggregatorID The id of the aggregator which should be used to aggregate the values corresponding to this
     * {@link EventKey}.
     * @param key The values of the keys.
     */
    public EventKey(int bucketID,
                    int schemaID,
                    int dimensionDescriptorID,
                    int aggregatorID,
                    GPOMutable key)
    {
      setBucketID(bucketID);
      setSchemaID(schemaID);
      setDimensionDescriptorID(dimensionDescriptorID);
      setAggregatorID(aggregatorID);
      setKey(key);
    }

    /**
     * Creates an event key with the given data. This constructor assumes that the bucketID will be 0.
     * @param schemaID The schemaID of the corresponding {@link DimensionalSchema}.
     * @param dimensionDescriptorID The dimensionDescriptorID of the corresponding {@link DimensionDescriptor}.
     * @param aggregatorID The id of the aggregator which should be used to aggregate the values corresponding to this
     * {@link EventKey}.
     * @param key The values of the keys.
     */
    public EventKey(int schemaID,
                    int dimensionDescriptorID,
                    int aggregatorID,
                    GPOMutable key)
    {
      setSchemaID(schemaID);
      setDimensionDescriptorID(dimensionDescriptorID);
      setAggregatorID(aggregatorID);
      setKey(key);
    }

    /**
     * Sets the dimension descriptor ID.
     * @param dimensionDescriptorID The dimension descriptor ID to set.
     */
    private void setDimensionDescriptorID(int dimensionDescriptorID)
    {
      this.dimensionDescriptorID = dimensionDescriptorID;
    }

    /**
     * Returns the dimension descriptor ID.
     * @return The dimension descriptor ID.
     */
    public int getDimensionDescriptorID()
    {
      return dimensionDescriptorID;
    }

    /**
     * Returns the aggregatorID.
     * @return The aggregatorID.
     */
    public int getAggregatorID()
    {
      return aggregatorID;
    }

    /**
     * Sets the aggregatorID.
     * @param aggregatorID The aggregatorID to set.
     */
    private void setAggregatorID(int aggregatorID)
    {
      this.aggregatorID = aggregatorID;
    }

    /**
     * Returns the schemaID.
     * @return The schemaID to set.
     */
    public int getSchemaID()
    {
      return schemaID;
    }

    /**
     * Sets the schemaID.
     * @param schemaID The schemaID to set.
     */
    private void setSchemaID(int schemaID)
    {
      this.schemaID = schemaID;
    }

    /**
     * Returns the key values.
     * @return The key values.
     */
    public GPOMutable getKey()
    {
      return key;
    }

    /**
     * Sets the bucektID.
     * @param bucketID The bucketID.
     */
    private void setBucketID(int bucketID)
    {
      this.bucketID = bucketID;
    }

    /**
     * Gets the bucketID.
     * @return The bucketID.
     */
    public int getBucketID()
    {
      return bucketID;
    }

    /**
     * Sets the key values.
     * @param key The key values to set.
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

  /**
   * This subclass of {@link DimensionsEvent} is used to represent input events for dimensions computation operators
   * that extend {@link AbstractDimensionsComputationFlexible}. {@link InputEvents} do not have data or functionality
   * that the {@link DimensionsEvent}s do not. The primary purpose behind this class is to make it clear to programmers
   * implementing {@link IncrementalAggregator}s that the inputs to their aggregators are not the same thing as the aggregates
   * themselves.
   */
  public static class InputEvent extends DimensionsEvent
  {
    private static final long serialVersionUID = 201505181028L;

    /**
     * Constructor for serialization.
     */
    private InputEvent()
    {
      //For kryo
    }

    /**
     * Creates an input event from the given {@link EventKey} and aggregates.
     * @param eventKey The event key.
     * @param aggregates The aggregate payload.
     */
    public InputEvent(EventKey eventKey,
                      GPOMutable aggregates)
    {
      super(eventKey,
            aggregates);
    }

    /**
     * Creates an {@link InputEvent} from the given information.
     * @param keys The key vlaues.
     * @param aggregates The aggregate values.
     * @param bucketID The bucketID.
     * @param schemaID The schemaID.
     * @param dimensionDescriptorID The dimensionDescriptor
     * @param aggregatorID The aggregatorID.
     */
    public InputEvent(GPOMutable keys,
                      GPOMutable aggregates,
                      int bucketID,
                      int schemaID,
                      int dimensionDescriptorID,
                      int aggregatorID)
    {
      super(keys,
            aggregates,
            bucketID,
            schemaID,
            dimensionDescriptorID,
            aggregatorID);
    }

    /**
     * Creates an {@link InputEvent} from the given information.
     * @param keys The key values.
     * @param aggregates The aggregate values.
     * @param schemaID The schemaID.
     * @param dimensionDescriptorID The dimensionsDescriptorID.
     * @param aggregatorID The aggregatorID.
     */
    public InputEvent(GPOMutable keys,
                      GPOMutable aggregates,
                      int schemaID,
                      int dimensionDescriptorID,
                      int aggregatorID)
    {
      super(keys,
            aggregates,
            schemaID,
            dimensionDescriptorID,
            aggregatorID);
    }
  }

  public static class Aggregate extends DimensionsEvent
  {
    private static final long serialVersionUID = 201505181028L;

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

    public static class AggregateHashingStrategy implements DTHashingStrategy<Aggregate>
    {
      public static final AggregateHashingStrategy INSTANCE = new AggregateHashingStrategy();

      private static final long serialVersionUID = 201505200426L;

      public AggregateHashingStrategy()
      {
      }

      @Override
      public boolean equals(Aggregate inputEventA, Aggregate inputEventB)
      {
        return inputEventA.getEventKey().equals(inputEventB.getEventKey());
      }

      @Override
      public int computeHashCode(Aggregate inputEvent)
      {
        return inputEvent.getEventKey().hashCode();
      }
    }
  }

  /**
   * This is an implementation of {@link DimensionsCombination} which is used for correctly mapping
   * {@link DimensionsEvent}s in dimensions computation operators extending {@link AbstractDimensionsComputationFlexible}.
   */
  public static class DimensionsEventDimensionsCombination implements DimensionsCombination<InputEvent, Aggregate>
  {
    private static final long serialVersionUID = 201505230842L;

    /**
     * Singleton pattern.
     */
    public static final DimensionsEventDimensionsCombination INSTANCE = new DimensionsEventDimensionsCombination();

    /**
     * Fore serialization and for singleton pattern.
     */
    private DimensionsEventDimensionsCombination()
    {
      //For kryo
    }

    @Override
    public void setKeys(InputEvent aggregatorInput, Aggregate aggregate)
    {
      //NOOP
    }

    @Override
    public boolean equals(InputEvent t, InputEvent t1)
    {
      return t.getEventKey().equals(t1.getEventKey());
    }

    @Override
    public int computeHashCode(InputEvent o)
    {
      return o.getEventKey().hashCode();
    }
  }
}
