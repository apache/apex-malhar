/**
 * Copyright (c) 2015 DataTorrent, Inc.
 * All rights reserved.
 */
package com.datatorrent.demos.machinedata.data;

import java.util.Map;

import com.google.common.collect.Maps;

import com.datatorrent.lib.appdata.gpo.GPOMutable;
import com.datatorrent.lib.appdata.gpo.GPOUtils;
import com.datatorrent.lib.appdata.schemas.FieldsDescriptor;
import com.datatorrent.lib.appdata.schemas.Type;
import com.datatorrent.lib.dimensions.DimensionsDescriptor;
import com.datatorrent.lib.dimensions.DimensionsEvent;
import com.datatorrent.lib.dimensions.DimensionsEvent.Aggregate;

/**
 * This is a custom aggregate to speed up computation for the machine demo.
 * @since 3.2.0
 */
public class MachineAggregate extends Aggregate
{
  public static final String CPU_USAGE = "CPU Usage (%)";
  public static final String RAM_USAGE = "RAM Usage (%)";
  public static final String HDD_USAGE = "HDD Usage (%)";

  private static final long serialVersionUID = 201510120355L;

  private int bucketID;
  private int schemaID;
  private int dimensionDescriptorID;
  private int aggregatorID;

  private String[] keysArray;

  public long cpuUsage;
  public long ramUsage;
  public long hddUsage;

  public long timestamp;
  public int timeBucket;

  public static final int NUM_COMBINATIONS = 6;

  public static final FieldsDescriptor AGGREGATES_DESCRIPTOR;
  public static final Map<Integer, FieldsDescriptor> COUNT_TO_DESCRIPTOR;

  static {
    Map<String, Type> fieldToType = Maps.newHashMap();
    fieldToType.put(CPU_USAGE, Type.LONG);
    fieldToType.put(RAM_USAGE, Type.LONG);
    fieldToType.put(HDD_USAGE, Type.LONG);

    AGGREGATES_DESCRIPTOR = new FieldsDescriptor(fieldToType);

    COUNT_TO_DESCRIPTOR = Maps.newHashMap();

    StringBuilder currentField = new StringBuilder("a");
    Map<String, Type> fieldToTypeTemp = Maps.newHashMap();
    fieldToTypeTemp.put(DimensionsDescriptor.DIMENSION_TIME, Type.LONG);
    fieldToTypeTemp.put(DimensionsDescriptor.DIMENSION_TIME_BUCKET, Type.INTEGER);
    COUNT_TO_DESCRIPTOR.put(0, new FieldsDescriptor(Maps.newHashMap(fieldToTypeTemp)));

    for (int counter = 1; counter < NUM_COMBINATIONS + 1; counter++) {
      fieldToTypeTemp.put(currentField.toString(), Type.STRING);
      COUNT_TO_DESCRIPTOR.put(counter, new FieldsDescriptor(Maps.newHashMap(fieldToTypeTemp)));

      currentField.append("a");
    }
  }

  protected MachineAggregate()
  {
    //for kryo
  }

  public MachineAggregate(String[] keysArray,
                          int bucketID,
                          int schemaID,
                          int dimensionDescriptorID,
                          int aggregatorID,
                          long cpuUsage,
                          long ramUsage,
                          long hddUsage,
                          long timestamp,
                          int timeBucket)
  {
    this.keysArray = keysArray;
    this.bucketID = bucketID;
    this.schemaID = schemaID;
    this.dimensionDescriptorID = dimensionDescriptorID;
    this.aggregatorID = aggregatorID;
    this.cpuUsage = cpuUsage;
    this.ramUsage = ramUsage;
    this.hddUsage = hddUsage;
    this.timestamp = timestamp;
    this.timeBucket = timeBucket;
  }

  /**
   * This is a helper method which returns the aggregates for this event.
   *
   * @return The helper method which returns the aggregates for this event.
   */
  @Override
  public GPOMutable getAggregates()
  {
    if (this.aggregates != null) {
      return aggregates;
    }

    aggregates = new GPOMutable(AGGREGATES_DESCRIPTOR);
    aggregates.getFieldsLong()[0] = cpuUsage;
    aggregates.getFieldsLong()[1] = hddUsage;
    aggregates.getFieldsLong()[2] = ramUsage;

    return aggregates;
  }

  /**
   * Returns the {@link EventKey} for this event.
   *
   * @return The {@link EventKey} for this event.
   */
  @Override
  public EventKey getEventKey()
  {
    if (eventKey != null) {
      return eventKey;
    }

    GPOMutable keys = new GPOMutable(COUNT_TO_DESCRIPTOR.get(keysArray.length));

    for (int keyCounter = 0; keyCounter < keysArray.length; keyCounter++) {
      keys.getFieldsString()[keyCounter] = keysArray[keyCounter];
    }

    keys.getFieldsLong()[0] = this.timestamp;
    keys.getFieldsInteger()[0] = this.timeBucket;

    eventKey = new EventKey(bucketID,
                            schemaID,
                            dimensionDescriptorID,
                            aggregatorID,
                            keys);

    return eventKey;
  }

  /**
   * This is a convenience method which returns the values of the key fields in this event's
   * {@link EventKey}.
   *
   * @return The values of the key fields in this event's {@link EventKey}.
   */
  @Override
  public GPOMutable getKeys()
  {
    getEventKey();
    return eventKey.getKey();
  }

  /**
   * @return the cpuUsage
   */
  public long getCpuUsage()
  {
    return cpuUsage;
  }

  /**
   * @param cpuUsage the cpuUsage to set
   */
  public void setCpuUsage(long cpuUsage)
  {
    this.cpuUsage = cpuUsage;
  }

  /**
   * @return the ramUsage
   */
  public long getRamUsage()
  {
    return ramUsage;
  }

  /**
   * @param ramUsage the ramUsage to set
   */
  public void setRamUsage(long ramUsage)
  {
    this.ramUsage = ramUsage;
  }

  /**
   * @return the hddUsage
   */
  public long getHddUsage()
  {
    return hddUsage;
  }

  /**
   * @param hddUsage the hddUsage to set
   */
  public void setHddUsage(long hddUsage)
  {
    this.hddUsage = hddUsage;
  }

  /**
   * @return the timestamp
   */
  public long getTimestamp()
  {
    return timestamp;
  }

  /**
   * @param timestamp the timestamp to set
   */
  public void setTimestamp(long timestamp)
  {
    this.timestamp = timestamp;
  }

  /**
   * @return the keysArray
   */
  public String[] getKeysArray()
  {
    return keysArray;
  }

  /**
   * @param keysArray the keysArray to set
   */
  public void setKeysArray(String[] keysArray)
  {
    this.keysArray = keysArray;
  }

  /**
   * @return the bucketID
   */
  @Override
  public int getBucketID()
  {
    return bucketID;
  }

  /**
   * @param bucketID the bucketID to set
   */
  public void setBucketID(int bucketID)
  {
    this.bucketID = bucketID;
  }

  /**
   * @return the schemaID
   */
  @Override
  public int getSchemaID()
  {
    return schemaID;
  }

  /**
   * @param schemaID the schemaID to set
   */
  public void setSchemaID(int schemaID)
  {
    this.schemaID = schemaID;
  }

  /**
   * @return the dimensionDescriptorID
   */
  @Override
  public int getDimensionDescriptorID()
  {
    return dimensionDescriptorID;
  }

  /**
   * @param dimensionDescriptorID the dimensionDescriptorID to set
   */
  public void setDimensionDescriptorID(int dimensionDescriptorID)
  {
    this.dimensionDescriptorID = dimensionDescriptorID;
  }

  /**
   * @return the aggregatorID
   */
  @Override
  public int getAggregatorID()
  {
    return aggregatorID;
  }

  /**
   * @param aggregatorID the aggregatorID to set
   */
  public void setAggregatorID(int aggregatorID)
  {
    this.aggregatorID = aggregatorID;
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

    if (getEventKey() != other.getEventKey() && (getEventKey() == null || !getEventKey().equals(other.getEventKey()))) {
      return false;
    }
    return true;
  }

}
