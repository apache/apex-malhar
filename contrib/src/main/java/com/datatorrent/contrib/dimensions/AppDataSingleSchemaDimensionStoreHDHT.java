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
package com.datatorrent.contrib.dimensions;

import com.datatorrent.api.Context.OperatorContext;
import com.datatorrent.lib.appdata.schemas.*;

import com.datatorrent.lib.dimensions.DimensionsDescriptor;
import com.datatorrent.lib.dimensions.DimensionsEvent.Aggregate;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import java.io.Serializable;
import javax.validation.constraints.NotNull;

import java.util.Map;
import java.util.Set;

import static com.datatorrent.lib.dimensions.AbstractDimensionsComputationFlexibleSingleSchema.DEFAULT_SCHEMA_ID;

/**
 * @displayName Simple App Data Dimensions Store
 * @category Store
 * @tags appdata, dimensions, store
 */
public class AppDataSingleSchemaDimensionStoreHDHT extends AbstractAppDataDimensionStoreHDHT implements Serializable
{
  private static final long serialVersionUID = 201505130939L;
  public static final long DEFAULT_BUCKET_ID = 0;

  @NotNull
  private String eventSchemaJSON;
  private String dimensionalSchemaJSON;

  @VisibleForTesting
  protected transient DimensionalConfigurationSchema eventSchema;
  private transient DimensionalSchema dimensionalSchema;
  private int schemaID = DEFAULT_SCHEMA_ID;
  private long bucketID = DEFAULT_BUCKET_ID;

  private boolean updateEnumValues = false;
  @SuppressWarnings({"rawtypes"})
  private Map<String, Set<Comparable>> seenEnumValues;

  @Override
  public void processEvent(Aggregate gae) {
    super.processEvent(gae);

    if(updateEnumValues) {
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
    return AppDataSingleSchemaDimensionStoreHDHT.DEFAULT_BUCKET_ID;
  }

  @Override
  public void setup(OperatorContext context)
  {
    super.setup(context);

    this.buckets = Sets.newHashSet(bucketID);

    if(!dimensionalSchema.isFixedFromTo()) {
      dimensionalSchema.setFrom(System.currentTimeMillis());
    }

    if(updateEnumValues) {
    if(seenEnumValues == null) {
      seenEnumValues = Maps.newHashMap();
      for(String key: eventSchema.getAllKeysDescriptor().getFieldList()) {
        @SuppressWarnings("rawtypes")
        Set<Comparable> enumValuesSet= Sets.newHashSet();
        seenEnumValues.put(key, enumValuesSet);
      }
    }
    }
  }

  @Override
  protected SchemaRegistry getSchemaRegistry()
  {
    eventSchema = new DimensionalConfigurationSchema(eventSchemaJSON, aggregatorRegistry);
    dimensionalSchema = new DimensionalSchema(schemaID, dimensionalSchemaJSON, eventSchema);

    return new SchemaRegistrySingle(dimensionalSchema);
  }


  @Override
  protected void processSchemaQuery(SchemaQuery schemaQuery)
  {
    dimensionalSchema.setTo(System.currentTimeMillis());

    if (updateEnumValues) {
      dimensionalSchema.setEnumsSetComparable(seenEnumValues);
    }

    SchemaResult schemaResult = schemaRegistry.getSchemaResult(schemaQuery);

    if (schemaResult != null) {
      String schemaResultJSON = resultSerializerFactory.serialize(schemaResult);
      queryResult.emit(schemaResultJSON);
    }
  }

  @Override
  public FieldsDescriptor getKeyDescriptor(int schemaID, int dimensionsDescriptorID)
  {
    return eventSchema.getDdIDToKeyDescriptor().get(dimensionsDescriptorID);
  }

  @Override
  public FieldsDescriptor getValueDescriptor(int schemaID, int dimensionsDescriptorID, int aggregatorID)
  {
    return eventSchema.getDdIDToAggIDToOutputAggDescriptor().get(dimensionsDescriptorID).get(aggregatorID);
  }

  @Override
  public long getBucketForSchema(int schemaID)
  {
    return bucketID;
  }

  /**
   * @param eventSchemaJSON the eventSchemaJSON to set
   */
  public void setEventSchemaJSON(String eventSchemaJSON)
  {
    this.eventSchemaJSON = eventSchemaJSON;
  }

  /**
   * @param dimensionalSchemaJSON the dimensionalSchemaJSON to set
   */
  public void setDimensionalSchemaJSON(String dimensionalSchemaJSON)
  {
    this.dimensionalSchemaJSON = dimensionalSchemaJSON;
  }

  /**
   * @return the updateEnumValues
   */
  public boolean isUpdateEnumValues()
  {
    return updateEnumValues;
  }

  /**
   * @param updateEnumValues the updateEnumValues to set
   */
  public void setUpdateEnumValues(boolean updateEnumValues)
  {
    this.updateEnumValues = updateEnumValues;
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
  public void setSchemaID(int schemaID)
  {
    this.schemaID = schemaID;
  }

  /**
   * @return the bucketID
   */
  public long getBucketID()
  {
    return bucketID;
  }

  /**
   * @param bucketID the bucketID to set
   */
  public void setBucketID(long bucketID)
  {
    this.bucketID = bucketID;
  }
}
