/**
 * Copyright (c) 2015 DataTorrent, Inc.
 * All rights reserved.
 */
package com.datatorrent.demos.dimensions.ads.stats;

import it.unimi.dsi.fastutil.ints.Int2IntOpenHashMap;
import it.unimi.dsi.fastutil.objects.Object2IntOpenHashMap;

import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datatorrent.lib.appdata.gpo.GPOMutable;
import com.datatorrent.lib.appdata.schemas.DimensionalConfigurationSchema;
import com.datatorrent.lib.appdata.schemas.FieldsDescriptor;
import com.datatorrent.lib.dimensions.AbstractDimensionsComputationFlexibleSingleSchema;
import com.datatorrent.lib.dimensions.DimensionsDescriptor;
import com.datatorrent.lib.dimensions.DimensionsEvent.Aggregate;
import com.datatorrent.lib.dimensions.DimensionsEvent.EventKey;
import com.datatorrent.lib.dimensions.aggregator.AggregatorRegistry;

import com.datatorrent.api.Context.OperatorContext;
import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.api.Operator;

import com.datatorrent.demos.dimensions.ads.AdInfo.AdInfoAggregateEvent;
import com.datatorrent.demos.dimensions.ads.InputItemGenerator;

/**
 * @category Test Bench
 * @since 3.1.0
 */
public class AdsConverter implements Operator
{
  private transient DimensionalConfigurationSchema dimensionsConfigurationSchema;
  private String eventSchemaJSON;
  private AggregatorRegistry aggregatorRegistry = AggregatorRegistry.DEFAULT_AGGREGATOR_REGISTRY;
  private transient Object2IntOpenHashMap<DimensionsDescriptor> dimensionsDescriptorToID = new Object2IntOpenHashMap<DimensionsDescriptor>();
  private transient FieldsDescriptor aggregateFieldsDescriptor;
  private String[] dimensionSpecs;
  private int schemaID = AbstractDimensionsComputationFlexibleSingleSchema.DEFAULT_SCHEMA_ID;
  private transient int sumAggregatorIndex;

  private Int2IntOpenHashMap prevDdIDToThisDdID = new Int2IntOpenHashMap();

  public final transient DefaultInputPort<AdInfoAggregateEvent> inputPort = new DefaultInputPort<AdInfoAggregateEvent>() {

    @Override
    public void process(AdInfoAggregateEvent tuple)
    {
      int ddID = prevDdIDToThisDdID.get(tuple.getDimensionsDescriptorID());
      FieldsDescriptor keyDescriptor = dimensionsConfigurationSchema.getDimensionsDescriptorIDToKeyDescriptor().get(ddID);

      GPOMutable key = new GPOMutable(keyDescriptor);

      for(String field: keyDescriptor.getFieldList()) {
        if(field.equals(InputItemGenerator.PUBLISHER)) {
          key.setField(InputItemGenerator.PUBLISHER, tuple.publisher);
        }
        else if(field.equals(InputItemGenerator.ADVERTISER)) {
          key.setField(InputItemGenerator.ADVERTISER, tuple.advertiser);
        }
        else if(field.equals(InputItemGenerator.LOCATION)) {
          key.setField(InputItemGenerator.LOCATION, tuple.location);
        }
      }

      key.setField(DimensionsDescriptor.DIMENSION_TIME, tuple.time);
      key.setField(DimensionsDescriptor.DIMENSION_TIME_BUCKET, tuple.timeBucket);

      EventKey eventKey = new EventKey(schemaID,
                                       ddID,
                                       sumAggregatorIndex,
                                       key);

      GPOMutable aggregates = new GPOMutable(aggregateFieldsDescriptor);
      aggregates.setField(InputItemGenerator.IMPRESSIONS, tuple.impressions);
      aggregates.setField(InputItemGenerator.COST, tuple.cost);
      aggregates.setField(InputItemGenerator.REVENUE,tuple.revenue);
      aggregates.setField(InputItemGenerator.CLICKS, tuple.clicks);

      outputPort.emit(new Aggregate(eventKey, aggregates));
    }
  };

  public final transient DefaultOutputPort<Aggregate> outputPort = new DefaultOutputPort<Aggregate>();

  public AdsConverter()
  {
  }

  @Override
  public void setup(OperatorContext context)
  {
    aggregatorRegistry.setup();

    dimensionsConfigurationSchema =
    new DimensionalConfigurationSchema(eventSchemaJSON,
                                       aggregatorRegistry);

    List<DimensionsDescriptor> dimensionsDescriptorList = dimensionsConfigurationSchema.getDimensionsDescriptorIDToDimensionsDescriptor();

    for(int ddID = 0;
        ddID < dimensionsDescriptorList.size();
        ddID++) {
      DimensionsDescriptor dimensionsDescriptor = dimensionsDescriptorList.get(ddID);
      dimensionsDescriptorToID.put(dimensionsDescriptor, ddID);
    }

    sumAggregatorIndex = aggregatorRegistry.getIncrementalAggregatorNameToID().get("SUM");
    aggregateFieldsDescriptor = dimensionsConfigurationSchema.getDimensionsDescriptorIDToAggregatorIDToOutputAggregatorDescriptor().
                                get(0).get(sumAggregatorIndex);

    for(int index = 0;
        index < dimensionSpecs.length;
        index++) {
      DimensionsDescriptor dimensionsDescriptor = new DimensionsDescriptor(dimensionSpecs[index]);
      LOG.debug("{}", dimensionsDescriptor);
      int newID = dimensionsDescriptorToID.get(dimensionsDescriptor);
      int oldID = index;
      LOG.debug("{} {}", newID, oldID);
      prevDdIDToThisDdID.put(newID, oldID);
    }
  }

  @Override
  public void beginWindow(long windowId)
  {
  }

  @Override
  public void endWindow()
  {
  }

  @Override
  public void teardown()
  {
  }

  /**
   * @return the aggregatorRegistry
   */
  public AggregatorRegistry getAggregatorRegistry()
  {
    return aggregatorRegistry;
  }

  /**
   * @param aggregatorRegistry the aggregatorRegistry to set
   */
  public void setAggregatorRegistry(AggregatorRegistry aggregatorRegistry)
  {
    this.aggregatorRegistry = aggregatorRegistry;
  }

  /**
   * @return the dimensionSpecs
   */
  public String[] getDimensionSpecs()
  {
    return dimensionSpecs;
  }

  /**
   * @param dimensionSpecs the dimensionSpecs to set
   */
  public void setDimensionSpecs(String[] dimensionSpecs)
  {
    this.dimensionSpecs = dimensionSpecs;
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
   * @return the eventSchemaJSON
   */
  public String getEventSchemaJSON()
  {
    return eventSchemaJSON;
  }

  /**
   * @param eventSchemaJSON the eventSchemaJSON to set
   */
  public void setEventSchemaJSON(String eventSchemaJSON)
  {
    this.eventSchemaJSON = eventSchemaJSON;
  }

  private static final Logger LOG = LoggerFactory.getLogger(AdsConverter.class);
}
