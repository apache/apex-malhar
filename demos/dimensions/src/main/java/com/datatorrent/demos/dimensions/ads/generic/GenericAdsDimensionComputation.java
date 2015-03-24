/*
 *  Copyright (c) 2012-2015 Malhar, Inc.
 *  All Rights Reserved.
 */

package com.datatorrent.demos.dimensions.ads.generic;

import com.datatorrent.api.Context.OperatorContext;
import com.datatorrent.demos.dimensions.ads.AggType;
import com.datatorrent.demos.dimensions.schemas.AdsSchemaResult;
import com.datatorrent.lib.appdata.dimensions.DimensionsAggregator;
import com.datatorrent.lib.appdata.dimensions.DimensionsDescriptor;
import com.datatorrent.lib.appdata.dimensions.GenericAggregateEvent;
import com.datatorrent.lib.appdata.dimensions.GenericDimensionsComputation;
import com.datatorrent.lib.appdata.dimensions.GenericEventSchema;
import com.datatorrent.lib.appdata.gpo.GPOImmutable;
import com.datatorrent.lib.appdata.gpo.GPOMutable;
import com.datatorrent.lib.appdata.schemas.FieldsDescriptor;
import com.google.common.collect.Lists;
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

  public GenericAdsDimensionComputation()
  {
  }

  @Override
  public void setup(OperatorContext context)
  {
    eventSchema = new GenericEventSchema(eventSchemaJSON);
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
      Map<String, FieldsDescriptor> map = eventSchema.getDdIDToAggregatorToAggregateDescriptor().get(index);

      for(Map.Entry<String, FieldsDescriptor> entry: map.entrySet()) {
        events.add(createGenericAggregateEvent(inputEvent,
                                               eventSchema.getDdIDToDD().get(index),
                                               keyFieldsDescriptor,
                                               entry.getValue(),
                                               index,
                                               AggType.valueOf(entry.getKey()).ordinal()));
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

    for(String field: keyFieldsDescriptor.getFields().getFields()) {
      if(field.equals(AdsSchemaResult.ADVERTISER)) {
        keyGPO.setField(field, ga.getAdvertiser());
      }
      else if(field.equals(AdsSchemaResult.PUBLISHER)) {
        keyGPO.setField(field, ga.getPublisher());
      }
      else if(field.equals(AdsSchemaResult.LOCATION)) {
        keyGPO.setField(field, ga.getLocation());
      }
      else if(field.equals(DimensionsDescriptor.DIMENSION_TIME)) {
        keyGPO.setField(field, ga.getTime());
      }
      else if(field.equals(DimensionsDescriptor.DIMENSION_TIME_BUCKET)) {
        keyGPO.setField(field, dd.getTimeBucket().ordinal());
      }
      else {
        throw new UnsupportedOperationException("This field is not supported: " + field);
      }
    }

    GPOMutable aggGPO = new GPOMutable(aggregateDescriptor);

    for(String field: aggregateDescriptor.getFields().getFields()) {
      if(field.equals(AdsSchemaResult.CLICKS)) {
        aggGPO.setField(AdsSchemaResult.CLICKS, ga.getClicks());
      }
      else if(field.equals(AdsSchemaResult.IMPRESSIONS)) {
        aggGPO.setField(AdsSchemaResult.IMPRESSIONS, ga.getImpressions());
      }
      else if(field.equals(AdsSchemaResult.REVENUE)) {
        aggGPO.setField(AdsSchemaResult.REVENUE, ga.getRevenue());
      }
      else if(field.equals(AdsSchemaResult.COST)) {
        aggGPO.setField(AdsSchemaResult.COST, ga.getCost());
      }
      else {
        throw new UnsupportedOperationException("This field is not supported: " + field);
      }
    }

    GenericAggregateEvent gae = new GenericAggregateEvent(new GPOImmutable(keyGPO),
                                                          aggGPO,
                                                          0,
                                                          dimensionDescriptorID,
                                                          aggregateID);
    return gae;
  }

  @Override
  public DimensionsAggregator<GenericAggregateEvent> getAggregator(int aggregatorID)
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
}
