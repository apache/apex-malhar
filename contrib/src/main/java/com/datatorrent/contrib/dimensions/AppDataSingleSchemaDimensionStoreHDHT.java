/**
 * Copyright (c) 2015 DataTorrent, Inc.
 * All rights reserved.
 */
package com.datatorrent.contrib.dimensions;

import java.io.Serializable;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentSkipListSet;

import javax.validation.constraints.NotNull;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

import com.datatorrent.api.Context.OperatorContext;
import com.datatorrent.api.annotation.OperatorAnnotation;
import com.datatorrent.contrib.hdht.AbstractSinglePortHDHTWriter;
import com.datatorrent.lib.appdata.schemas.*;
import com.datatorrent.lib.dimensions.AbstractDimensionsComputationFlexibleSingleSchema;
import com.datatorrent.lib.dimensions.DimensionsDescriptor;
import com.datatorrent.lib.dimensions.DimensionsEvent.Aggregate;

/**
 * This is a dimensions store which stores data corresponding to one {@link DimensionalSchema} into an HDHT bucket.
 * This operator requires an upstream dimensions computation operator, to produce {@link Aggregate}s with the same
 * link {@link DimensionalSchema} and schemaID.
 *
 * @displayName Simple App Data Dimensions Store
 * @category DT View Integration
 * @tags app data, dimensions, store
 * @since 3.1.0
 *
 */
@OperatorAnnotation(checkpointableWithinAppWindow = false)
public class AppDataSingleSchemaDimensionStoreHDHT extends AbstractAppDataDimensionStoreHDHT implements Serializable
{
  private static final long serialVersionUID = 201505130939L;
  /**
   * This is the id of the default bucket that data is stored into.
   */
  public static final long DEFAULT_BUCKET_ID = 0;
  /**
   * This is the JSON which defines this operator's {@link DimensionalConfigurationSchema}.
   */
  @NotNull
  private String configurationSchemaJSON;
  /**
   * This is the JSON which defines the schema stub for this operator's {@link DimensionalSchema}.
   */
  private String dimensionalSchemaStubJSON;
  /**
   * This operator's {@link DimensionalConfigurationSchema}.
   */
  @VisibleForTesting
  protected transient DimensionalConfigurationSchema configurationSchema;
  /**
   * This operator's {@link DimensionalSchema}.
   */
  protected transient DimensionalSchema dimensionalSchema;
  /**
   * The {@link schemaID} of for data stored by this operator.
   */
  private int schemaID = AbstractDimensionsComputationFlexibleSingleSchema.DEFAULT_SCHEMA_ID;
  /**
   * The ID of the HDHT bucket that this operator stores data in.
   */
  private long bucketID = DEFAULT_BUCKET_ID;
  /**
   * This flag determines whether or not the lists of all possible values for the keys in this operators {@link DimensionalSchema}
   * are updated based on the key values seen in {@link Aggregate}s received by this operator.
   */
  protected boolean updateEnumValues = false;
  @SuppressWarnings({"rawtypes"})
  /**
   * This is a map that stores the seen values of all the keys in this operator's {@link DimensionalSchema}. The
   * key in this map is the name of a key. The value in this map is the set of all values this operator has seen for
   * that key.
   */
  protected Map<String, Set<Comparable>> seenEnumValues;

  @Override
  public void processEvent(Aggregate gae) {
    super.processEvent(gae);

    if(!dimensionalSchema.isPredefinedFromTo() &&
       gae.getKeys().getFieldDescriptor().getFields().getFields().contains(DimensionsDescriptor.DIMENSION_TIME)) {

      long timestamp = gae.getEventKey().getKey().getFieldLong(DimensionsDescriptor.DIMENSION_TIME);

      if (getMinTimestamp() == null || timestamp < getMinTimestamp()) {
        setMinTimestamp(timestamp);
        dimensionalSchema.setFrom(timestamp);
      }

      if (getMaxTimestamp() == null || timestamp > getMaxTimestamp()) {
        setMaxTimestamp(timestamp);
        dimensionalSchema.setTo(timestamp);
      }
    }

    if(updateEnumValues) {
      //update the lists of possible values for keys in this operator's {@link DimensionalSchema}.
      for(String field: gae.getKeys().getFieldDescriptor().getFields().getFields()) {
        if(DimensionsDescriptor.RESERVED_DIMENSION_NAMES.contains(field)) {
          continue;
        }

        @SuppressWarnings("rawtypes")
        Comparable fieldValue = (Comparable)gae.getKeys().getField(field);
        seenEnumValues.get(field).add(fieldValue);
      }
    }
  }

  @Override
  protected long getBucketKey(Aggregate event)
  {
    return bucketID;
  }

  @Override
  public int getPartitionGAE(Aggregate inputEvent) {
    return inputEvent.getDimensionDescriptorID();
  }

  @Override
  public void setup(OperatorContext context)
  {
    boolean initializeSeenEnumValues = seenEnumValues == null;

    if (initializeSeenEnumValues) {
      seenEnumValues = Maps.newConcurrentMap();
    }

    super.setup(context);

    this.buckets = Sets.newHashSet(bucketID);

    if (!dimensionalSchema.isPredefinedFromTo()) {
      if (getMinTimestamp() != null) {
        dimensionalSchema.setFrom(getMinTimestamp());
      }

      if (getMaxTimestamp() != null) {
        dimensionalSchema.setTo(getMaxTimestamp());
      }
    }

    if (initializeSeenEnumValues) {
      Map<String, List<Object>> keysToEnumValuesList = this.configurationSchema.getKeysToEnumValuesList();

      for (String key : configurationSchema.getKeyDescriptor().getFieldList()) {
        if (DimensionsDescriptor.RESERVED_DIMENSION_NAMES.contains(key)) {
          continue;
        }

        @SuppressWarnings("rawtypes")
        Set<Comparable> enumValuesSet = new ConcurrentSkipListSet<>();
        @SuppressWarnings({"unchecked", "rawtypes"})
        List<Comparable> enumValuesList = (List)keysToEnumValuesList.get(key);
        enumValuesSet.addAll(enumValuesList);
        seenEnumValues.put(key, enumValuesSet);
      }
    }
  }

  @Override
  public Collection<Partition<AbstractSinglePortHDHTWriter<Aggregate>>> definePartitions(Collection<Partition<AbstractSinglePortHDHTWriter<Aggregate>>> partitions, PartitioningContext context)
  {
    Collection<Partition<AbstractSinglePortHDHTWriter<Aggregate>>> newPartitions = super.definePartitions(partitions, context);

    if (newPartitions.size() == partitions.size()) {
      return newPartitions;
    }

    Iterator<Partition<AbstractSinglePortHDHTWriter<Aggregate>>> iterator = newPartitions.iterator();

    long bucket = ((AppDataSingleSchemaDimensionStoreHDHT)iterator.next().getPartitionedInstance()).getBucketID();
    long last = bucket + newPartitions.size();

    for (; ++bucket < last;) {
      ((AppDataSingleSchemaDimensionStoreHDHT)iterator.next().getPartitionedInstance()).setBucketID(bucket);
    }

    return newPartitions;
  }

  @Override
  protected SchemaRegistry getSchemaRegistry()
  {
    configurationSchema = new DimensionalConfigurationSchema(configurationSchemaJSON, aggregatorRegistry);
    dimensionalSchema = new DimensionalSchema(schemaID, dimensionalSchemaStubJSON, configurationSchema);

    return new SchemaRegistrySingle(dimensionalSchema);
  }

  @Override
  protected SchemaResult processSchemaQuery(SchemaQuery schemaQuery)
  {
    if(updateEnumValues) {
      //update the enum values in the schema.
      dimensionalSchema.setEnumsSetComparable(seenEnumValues);
    }

    return schemaRegistry.getSchemaResult(schemaQuery);
  }

  @Override
  @SuppressWarnings("unchecked")
  protected DimensionsQueueManager getDimensionsQueueManager()
  {
    return new DimensionsQueueManager(this, schemaRegistry, new SimpleDataQueryDimensionalExpander((Map) seenEnumValues));
  }

  @Override
  public FieldsDescriptor getKeyDescriptor(int schemaID, int dimensionsDescriptorID)
  {
    return configurationSchema.getDimensionsDescriptorIDToKeyDescriptor().get(dimensionsDescriptorID);
  }

  @Override
  public FieldsDescriptor getValueDescriptor(int schemaID, int dimensionsDescriptorID, int aggregatorID)
  {
    return configurationSchema.getDimensionsDescriptorIDToAggregatorIDToOutputAggregatorDescriptor().get(dimensionsDescriptorID).get(aggregatorID);
  }

  @Override
  public long getBucketForSchema(int schemaID)
  {
    return bucketID;
  }

  /**
   * Sets the JSON representing the {@link DimensionalConfigurationSchema} for this operator.
   * @param configurationSchemaJSON The JSON representing the {@link DimensionalConfigurationSchema} for this operator.
   */
  public void setConfigurationSchemaJSON(String configurationSchemaJSON)
  {
    this.configurationSchemaJSON = configurationSchemaJSON;
  }

  /**
   * Gets the JSON representing the {@link DimensionalConfigurationSchema} for this operator.
   * @return The JSON representing the {@link DimensionalConfigurationSchema} for this operator.
   */
  public String getConfigurationSchemaJSON()
  {
    return configurationSchemaJSON;
  }

  /**
   * Sets the JSON representing the dimensional schema stub to be used by this operator's {@link DimensionalSchema}.
   * @param dimensionalSchemaStubJSON The JSON representing the dimensional schema stub to be used by this operator's {@link DimensionalSchema}.
   */
  public void setDimensionalSchemaStubJSON(String dimensionalSchemaStubJSON)
  {
    this.dimensionalSchemaStubJSON = dimensionalSchemaStubJSON;
  }

  /**
   * Gets the JSON representing the dimensional schema stub to be used by this operator's {@link DimensionalSchema}.
   * @return The JSON representing the dimensional schema stub to be used by this operator's {@link DimensionalSchema}.
   */
  public String getDimensionalSchemaStubJSON()
  {
    return dimensionalSchemaStubJSON;
  }

  /**
   * Returns the value of updateEnumValues.
   * @return The value of updateEnumValues.
   */
  public boolean isUpdateEnumValues()
  {
    return updateEnumValues;
  }

  /**
   * Sets the value of updateEnumValues. This value is true if the list of possible key values in this operator's {@link DimensionalSchema} is to be updated
   * based on observed values of the keys. This value is false if the possible key values in this operator's {@link DimensionalSchema}
   * are not to be updated.
   * @param updateEnumValues The value of updateEnumValues to set.
   */
  public void setUpdateEnumValues(boolean updateEnumValues)
  {
    this.updateEnumValues = updateEnumValues;
  }

  /**
   * Returns the schemaID of data stored by this operator.
   * @return The schemaID of data stored by this operator.
   */
  public int getSchemaID()
  {
    return schemaID;
  }

  /**
   * Sets the schemaId for the schema stored and served by this operator.
   * @param schemaID the schemaID to set
   */
  public void setSchemaID(int schemaID)
  {
    this.schemaID = schemaID;
  }

  /**
   * Gets the id of the bucket that this operator will store all its information in.
   * @return The id of the bucket that this operator will store all its information in.
   */
  public long getBucketID()
  {
    return bucketID;
  }

  /**
   * Sets the id of the bucket that this operator will store all its information in.
   * @param bucketID The id of the bucket that this operator will store all its information in.
   */
  public void setBucketID(long bucketID)
  {
    this.bucketID = bucketID;
  }
}
