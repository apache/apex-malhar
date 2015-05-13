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

import java.io.IOException;
import java.io.Serializable;

import java.util.Map;
import java.util.Set;

import javax.validation.constraints.NotNull;

import org.apache.commons.lang.mutable.MutableBoolean;
import org.apache.commons.lang3.mutable.MutableLong;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

import com.datatorrent.api.AppData;
import com.datatorrent.api.Context.OperatorContext;
import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.api.annotation.InputPortFieldAnnotation;

import com.datatorrent.lib.appdata.dimensions.*;
import com.datatorrent.lib.appdata.qr.Data;
import com.datatorrent.lib.appdata.qr.DataDeserializerFactory;
import com.datatorrent.lib.appdata.qr.DataSerializerFactory;
import com.datatorrent.lib.appdata.qr.Result;
import com.datatorrent.lib.appdata.qr.processor.QueryProcessor;
import com.datatorrent.lib.appdata.schemas.*;

import static com.datatorrent.lib.appdata.dimensions.DimensionsComputationSingleSchema.DEFAULT_SCHEMA_ID;

/**
 * @displayName Simple App Data Dimensions Store
 * @category Store
 * @tags appdata, dimensions, store
 */
public class AppDataSingleSchemaDimensionStoreHDHT extends DimensionsStoreHDHT implements Serializable
{
  private static final long serialVersionUID = 201503231218L;

  public static final long DEFAULT_BUCKET_ID = 0;

  @NotNull
  private String eventSchemaJSON;
  private String dimensionalSchemaJSON;

  @VisibleForTesting
  protected transient DimensionalEventSchema eventSchema;
  private transient SchemaDimensional dimensionalSchema;
  private int schemaID = DEFAULT_SCHEMA_ID;

  //Query Processing - Start
  private transient QueryProcessor<DataQueryDimensional, QueryMeta, MutableLong, MutableBoolean, Result> queryProcessor;
  private final transient DataDeserializerFactory queryDeserializerFactory;
  @NotNull
  private AppDataFormatter appDataFormatter = new AppDataFormatter();
  private transient SchemaRegistry schemaRegistry;
  @NotNull
  private AggregatorInfo aggregatorInfo = AggregatorUtils.DEFAULT_AGGREGATOR_INFO;
  private transient DataSerializerFactory resultSerializerFactory;
  //Query Processing - End

  private boolean updateEnumValues = false;
  @SuppressWarnings({"rawtypes"})
  private Map<String, Set<Comparable>> seenEnumValues;

  @AppData.ResultPort
  public final transient DefaultOutputPort<String> queryResult = new DefaultOutputPort<String>();

  @InputPortFieldAnnotation(optional = true)
  @AppData.QueryPort
  public transient final DefaultInputPort<String> query = new DefaultInputPort<String>()
  {
    @Override public void process(String s)
    {
      logger.debug("Received {}", s);
      Data query;
      try {
        query = queryDeserializerFactory.deserialize(s);
      }
      catch(IOException ex) {
        logger.error("error parsing query: {}", s);
        logger.error("{}", ex);
        return;
      }

      if(query instanceof SchemaQuery) {
        dimensionalSchema.setTo(System.currentTimeMillis());

        if(updateEnumValues) {
          dimensionalSchema.setEnumsSetComparable(seenEnumValues);
        }

        SchemaResult schemaResult = schemaRegistry.getSchemaResult((SchemaQuery) query);

        if(schemaResult != null) {
          String schemaResultJSON = resultSerializerFactory.serialize(schemaResult);
          logger.info("Emitter {}", schemaResultJSON);
          queryResult.emit(schemaResultJSON);
        }
      }
      else if(query instanceof DataQueryDimensional) {
        DataQueryDimensional gdq = (DataQueryDimensional) query;
        queryProcessor.enqueue(gdq, null, null);
      }
      else {
        logger.error("Invalid query {}", s);
      }
    }
  };

  @SuppressWarnings("unchecked")
  public AppDataSingleSchemaDimensionStoreHDHT()
  {
    queryDeserializerFactory = new DataDeserializerFactory(SchemaQuery.class, DataQueryDimensional.class);
  }

  @Override
  public void processEvent(AggregateEvent gae) {
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
  protected long getBucketKey(AggregateEvent event)
  {
    return AppDataSingleSchemaDimensionStoreHDHT.DEFAULT_BUCKET_ID;
  }

  @Override
  public void setup(OperatorContext context)
  {
    aggregatorInfo.setup();

    eventSchema = new DimensionalEventSchema(eventSchemaJSON,
                                             aggregatorInfo);
    dimensionalSchema = new SchemaDimensional(schemaID,
                                              dimensionalSchemaJSON,
                                              eventSchema);

    schemaRegistry = new SchemaRegistrySingle(dimensionalSchema);
    queryProcessor = QueryProcessor.newInstance(new DimensionsQueryComputer(this, schemaRegistry),
      new DimensionsQueryQueueManager(this, schemaRegistry));

    resultSerializerFactory = new DataSerializerFactory(appDataFormatter);
    queryDeserializerFactory.setContext(DataQueryDimensional.class, schemaRegistry);
    super.setup(context);

    if(!dimensionalSchema.isFixedFromTo()) {
      dimensionalSchema.setFrom(System.currentTimeMillis());
    }

    //seenEnumValues
    if(seenEnumValues == null) {
      seenEnumValues = Maps.newHashMap();
      for(String key: eventSchema.getAllKeysDescriptor().getFieldList()) {
        @SuppressWarnings("rawtypes")
        Set<Comparable> enumValuesSet= Sets.newHashSet();
        seenEnumValues.put(key, enumValuesSet);
      }
    }
  }

  @Override
  public void beginWindow(long windowId)
  {
    queryProcessor.beginWindow(windowId);
    super.beginWindow(windowId);
  }

  @Override
  public void endWindow()
  {
    super.endWindow();

    MutableBoolean done = new MutableBoolean(false);

    while(done.isFalse()) {
      Result aotr = queryProcessor.process(done);

      if(aotr != null) {
        String result = resultSerializerFactory.serialize(aotr);
        logger.debug("Emitting the result: {}", result);
        queryResult.emit(result);
      }
    }

    queryProcessor.endWindow();
  }

  @Override
  public void teardown()
  {
    queryProcessor.teardown();
    super.teardown();
  }

  @Override
  public DimensionsStaticAggregator getAggregator(int aggregatorID)
  {
    return aggregatorInfo.getStaticAggregatorIDToAggregator().get(aggregatorID);
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
    return DEFAULT_BUCKET_ID;
  }

  @Override
  public int getPartitionGAE(AggregateEvent inputEvent)
  {
    return inputEvent.getEventKey().hashCode();}

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

  @Override
  protected int getAggregatorID(String aggregatorName)
  {
    return AggregatorStaticType.NAME_TO_ORDINAL.get(aggregatorName);
  }

  public void setAppDataFormatter(AppDataFormatter appDataFormatter)
  {
    this.appDataFormatter = appDataFormatter;
  }

  /**
   * @return the appDataFormatter
   */
  public AppDataFormatter getAppDataFormatter()
  {
    return appDataFormatter;
  }

  /**
   * @return the aggregatorInfo
   */
  public AggregatorInfo getAggregatorInfo()
  {
    return aggregatorInfo;
  }

  /**
   * @param aggregatorInfo the aggregatorInfo to set
   */
  public void setAggregatorInfo(@NotNull AggregatorInfo aggregatorInfo)
  {
    this.aggregatorInfo = aggregatorInfo;
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

  private static final Logger logger = LoggerFactory.getLogger(AppDataSingleSchemaDimensionStoreHDHT.class);
}
