/*
 *  Copyright (c) 2012-2015 Malhar, Inc.
 *  All Rights Reserved.
 */

package com.datatorrent.demos.dimensions.ads.generic;

import com.datatorrent.api.Context.OperatorContext;
import com.datatorrent.demos.dimensions.ads.schemas.AdsSchemaResult;
import com.datatorrent.lib.appdata.dimensions.AggType;
import com.datatorrent.lib.appdata.dimensions.DimensionsDescriptor;
import com.datatorrent.lib.appdata.dimensions.GenericAggregateEvent;
import com.datatorrent.lib.appdata.dimensions.GenericDimensionsAggregator;
import com.datatorrent.lib.appdata.dimensions.GenericDimensionsComputation;
import com.datatorrent.lib.appdata.dimensions.GenericEventSchema;
import com.datatorrent.lib.appdata.gpo.GPOImmutable;
import com.datatorrent.lib.appdata.gpo.GPOMutable;
import com.datatorrent.lib.appdata.schemas.FieldsDescriptor;
import com.google.common.collect.Lists;
import it.unimi.dsi.fastutil.ints.Int2ObjectMap;
import it.unimi.dsi.fastutil.ints.Int2ObjectOpenHashMap;
import javax.validation.constraints.NotNull;

import java.util.List;
import java.util.Map;

/**
 *
 * @author Timothy Farkas: tim@datatorrent.com
 */
public class GenericAdsDimensionComputation extends GenericDimensionsComputation<GenericAdInfo>
{
  @NotNull
  private String eventSchemaJSON;

  private transient GenericEventSchema eventSchema;
  private transient List<Int2ObjectMap<FieldsDescriptor>> ddIDToAggIDToInputAggDescriptor;
  private transient List<Int2ObjectMap<FieldsDescriptor>> ddIDToAggIDToOutputAggDescriptor;

  public GenericAdsDimensionComputation()
  {
  }

  @Override
  public void setup(OperatorContext context)
  {
    eventSchema = new GenericEventSchema(eventSchemaJSON);

    List<Map<String, FieldsDescriptor>> tempDescriptorList = eventSchema.getDdIDToAggregatorToAggregateDescriptor();
    ddIDToAggIDToInputAggDescriptor = Lists.newArrayList();
    ddIDToAggIDToOutputAggDescriptor = Lists.newArrayList();

    for(int index = 0;
        index < tempDescriptorList.size();
        index++) {
      Int2ObjectMap<FieldsDescriptor> inputMap = new Int2ObjectOpenHashMap<FieldsDescriptor>();
      Int2ObjectMap<FieldsDescriptor> outputMap = new Int2ObjectOpenHashMap<FieldsDescriptor>();

      ddIDToAggIDToInputAggDescriptor.add(inputMap);
      ddIDToAggIDToOutputAggDescriptor.add(outputMap);

      for(Map.Entry<String, FieldsDescriptor> entry:
          tempDescriptorList.get(index).entrySet()) {
        String aggregatorName = entry.getKey();
        FieldsDescriptor inputDescriptor = entry.getValue();
        AggType aggType = AggType.valueOf(aggregatorName);
        inputMap.put(aggType.ordinal(), entry.getValue());
        outputMap.put(aggType.ordinal(),
                      aggType.getAggregator().getResultDescriptor(inputDescriptor));
      }
    }

    super.setup(context);
  }

  @Override
  public GenericAggregateEvent[] convertInputEvent(GenericAdInfo inputEvent)
  {
    List<GenericAggregateEvent> events = Lists.newArrayList();
    List<FieldsDescriptor> keyFieldsDescriptors = eventSchema.getDdIDToKeyDescriptor();

    for(int index = 0;
        index < keyFieldsDescriptors.size();
        index++) {
      FieldsDescriptor keyFieldsDescriptor = keyFieldsDescriptors.get(index);
      Int2ObjectMap<FieldsDescriptor> map = ddIDToAggIDToInputAggDescriptor.get(index);

      for(int mapIndex = 0;
          mapIndex < map.size();
          mapIndex++) {
        events.add(createGenericAggregateEvent(inputEvent,
                                               eventSchema.getDdIDToDD().get(index),
                                               keyFieldsDescriptor,
                                               map.get(mapIndex),
                                               index,
                                               mapIndex));
      }
    }

    GenericAggregateEvent[] gaes = new GenericAggregateEvent[events.size()];

    for(int gaeIndex = 0;
        gaeIndex < events.size();
        gaeIndex++) {
      gaes[gaeIndex] = events.get(gaeIndex);
    }

    return gaes;
  }

  private GenericAggregateEvent createGenericAggregateEvent(GenericAdInfo ga,
                                                            DimensionsDescriptor dd,
                                                            FieldsDescriptor keyFieldsDescriptor,
                                                            FieldsDescriptor aggregateDescriptor,
                                                            int dimensionDescriptorID,
                                                            int aggregateID)
  {
    GPOMutable keyGPO = new GPOMutable(keyFieldsDescriptor);

    keyGPO.setField(AdsSchemaResult.ADVERTISER, ga.getAdvertiser());
    keyGPO.setField(AdsSchemaResult.PUBLISHER, ga.getPublisher());
    keyGPO.setField(AdsSchemaResult.LOCATION, ga.getLocation());

    if(dd.getTimeBucket() == null) {
      keyGPO.setField(DimensionsDescriptor.DIMENSION_TIME, ga.getTime());
    }
    else {
      keyGPO.setField(DimensionsDescriptor.DIMENSION_TIME, dd.getTimeBucket().roundDown(ga.getTime()));
    }

    keyGPO.setField(DimensionsDescriptor.DIMENSION_TIME_BUCKET, dd.getTimeBucket().ordinal());

    GPOMutable aggGPO = new GPOMutable(aggregateDescriptor);

    aggGPO.setField(AdsSchemaResult.CLICKS, ga.getClicks());
    aggGPO.setField(AdsSchemaResult.IMPRESSIONS, ga.getImpressions());
    aggGPO.setField(AdsSchemaResult.REVENUE, ga.getRevenue());
    aggGPO.setField(AdsSchemaResult.COST, ga.getCost());

    GenericAggregateEvent gae = new GenericAggregateEvent(new GPOImmutable(keyGPO),
                                                          aggGPO,
                                                          0,
                                                          dimensionDescriptorID,
                                                          aggregateID);
    return gae;
  }

  @Override
  public GenericDimensionsAggregator getAggregator(int aggregatorID)
  {
    return AggType.values()[aggregatorID].getAggregator();
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

  @Override
  public FieldsDescriptor getAggregateFieldsDescriptor(int schemaID,
                                                       int dimensionDescriptorID,
                                                       int aggregatorID)
  {
    return ddIDToAggIDToOutputAggDescriptor.get(dimensionDescriptorID).get(aggregatorID);
  }
}
