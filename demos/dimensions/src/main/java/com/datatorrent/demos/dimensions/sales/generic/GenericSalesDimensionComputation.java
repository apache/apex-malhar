/*
 *  Copyright (c) 2012-2015 Malhar, Inc.
 *  All Rights Reserved.
 */

package com.datatorrent.demos.dimensions.sales.generic;

import com.datatorrent.api.Context.OperatorContext;
import com.datatorrent.lib.appdata.dimensions.AggType;
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
public class GenericSalesDimensionComputation extends GenericDimensionsComputation<Map<String,Object>>
{
  @NotNull
  private String eventSchemaJSON;

  private transient GenericEventSchema eventSchema;

  public GenericSalesDimensionComputation()
  {
  }

  @Override
  public void setup(OperatorContext context)
  {
    eventSchema = new GenericEventSchema(eventSchemaJSON);
    super.setup(context);
  }

  @Override
  public GenericAggregateEvent[] convertInputEvent(Map<String, Object> inputEvent)
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

  private GenericAggregateEvent createGenericAggregateEvent(Map<String, Object> ga,
                                                            DimensionsDescriptor dd,
                                                            FieldsDescriptor keyFieldsDescriptor,
                                                            FieldsDescriptor aggregateDescriptor,
                                                            int dimensionDescriptorID,
                                                            int aggregateID)
  {
    GPOMutable keyGPO = new GPOMutable(keyFieldsDescriptor);

    for(String field: keyFieldsDescriptor.getFields().getFields()) {
      if(field.equals(JsonSalesGenerator.KEY_CHANNEL)) {
        keyGPO.setField(field, ga.get(JsonSalesGenerator.KEY_CHANNEL));
      }
      else if(field.equals(JsonSalesGenerator.KEY_CUSTOMER)) {
        keyGPO.setField(field, ga.get(JsonSalesGenerator.KEY_CUSTOMER));
      }
      else if(field.equals(JsonSalesGenerator.KEY_PRODUCT)) {
        keyGPO.setField(field, ga.get(JsonSalesGenerator.KEY_PRODUCT));
      }
      else if(field.equals(JsonSalesGenerator.KEY_REGION)) {
        keyGPO.setField(field, ga.get(JsonSalesGenerator.KEY_REGION));
      }
      else if(field.equals(DimensionsDescriptor.DIMENSION_TIME)) {
        if(dd.getTimeBucket() == null) {
          keyGPO.setField(field, ga.get(DimensionsDescriptor.DIMENSION_TIME));
        }
        else {
          keyGPO.setField(field, dd.getTimeBucket().roundDown((Long) ga.get(DimensionsDescriptor.DIMENSION_TIME)));
        }
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
      if(field.equals(JsonSalesGenerator.AGG_AMOUNT)) {
        aggGPO.setField(field, ga.get(JsonSalesGenerator.AGG_AMOUNT));
      }
      else if(field.equals(JsonSalesGenerator.AGG_DISCOUNT)) {
        aggGPO.setField(field, ga.get(JsonSalesGenerator.AGG_DISCOUNT));
      }
      else if(field.equals(JsonSalesGenerator.AGG_TAX)) {
        aggGPO.setField(field, ga.get(JsonSalesGenerator.AGG_TAX));
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
