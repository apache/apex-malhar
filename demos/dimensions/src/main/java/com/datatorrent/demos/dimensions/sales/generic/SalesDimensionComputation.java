/*
 *  Copyright (c) 2012-2015 Malhar, Inc.
 *  All Rights Reserved.
 */

package com.datatorrent.demos.dimensions.sales.generic;

import com.datatorrent.api.Context.OperatorContext;
import com.datatorrent.lib.appdata.dimensions.AggregatorType;
import com.datatorrent.lib.appdata.dimensions.DimensionsDescriptor;
import com.datatorrent.lib.appdata.dimensions.AggregateEvent;
import com.datatorrent.lib.appdata.dimensions.DimensionsAggregator;
import com.datatorrent.lib.appdata.dimensions.DimensionsComputation;
import com.datatorrent.lib.appdata.schemas.DimensionalEventSchema;
import com.datatorrent.lib.appdata.gpo.GPOImmutable;
import com.datatorrent.lib.appdata.gpo.GPOMutable;
import com.datatorrent.lib.appdata.schemas.FieldsDescriptor;
import com.google.common.collect.Lists;
import it.unimi.dsi.fastutil.ints.Int2ObjectMap;
import it.unimi.dsi.fastutil.ints.Int2ObjectOpenHashMap;
import it.unimi.dsi.fastutil.ints.IntArrayList;
import javax.validation.constraints.NotNull;

import java.util.List;
import java.util.Map;

/**
 *
 * @author Timothy Farkas: tim@datatorrent.com
 */
public class SalesDimensionComputation extends DimensionsComputation<Map<String,Object>>
{
  @NotNull
  private String eventSchemaJSON;

  private transient DimensionalEventSchema eventSchema;
  private transient List<Int2ObjectMap<FieldsDescriptor>> ddIDToAggIDToInputAggDescriptor;
  private transient List<Int2ObjectMap<FieldsDescriptor>> ddIDToAggIDToOutputAggDescriptor;
  private transient List<IntArrayList> ddIDToAggIDs;

  public SalesDimensionComputation()
  {
  }

  @Override
  public void setup(OperatorContext context)
  {
    eventSchema = new DimensionalEventSchema(eventSchemaJSON);

    List<Map<String, FieldsDescriptor>> tempDescriptorList = eventSchema.getDdIDToAggregatorToAggregateDescriptor();
    ddIDToAggIDs = Lists.newArrayList();
    ddIDToAggIDToInputAggDescriptor = Lists.newArrayList();
    ddIDToAggIDToOutputAggDescriptor = Lists.newArrayList();

    for(int index = 0;
        index < tempDescriptorList.size();
        index++) {
      IntArrayList aggIDList = new IntArrayList();
      Int2ObjectMap<FieldsDescriptor> inputMap = new Int2ObjectOpenHashMap<FieldsDescriptor>();
      Int2ObjectMap<FieldsDescriptor> outputMap = new Int2ObjectOpenHashMap<FieldsDescriptor>();

      ddIDToAggIDs.add(aggIDList);
      ddIDToAggIDToInputAggDescriptor.add(inputMap);
      ddIDToAggIDToOutputAggDescriptor.add(outputMap);

      for(Map.Entry<String, FieldsDescriptor> entry
          : tempDescriptorList.get(index).entrySet()) {
        String aggregatorName = entry.getKey();
        FieldsDescriptor inputDescriptor = entry.getValue();
        AggregatorType aggType = AggregatorType.valueOf(aggregatorName);
        aggIDList.add(aggType.ordinal());
        inputMap.put(aggType.ordinal(), inputDescriptor);
        outputMap.put(aggType.ordinal(),
                      aggType.getAggregator().getResultDescriptor(inputDescriptor));
      }
    }

    super.setup(context);
  }

  @Override
  public AggregateEvent[] convertInputEvent(Map<String, Object> inputEvent)
  {
    List<AggregateEvent> events = Lists.newArrayList();
    List<FieldsDescriptor> keyFieldsDescriptors = eventSchema.getDdIDToKeyDescriptor();

    for(int index = 0;
        index < keyFieldsDescriptors.size();
        index++) {
      FieldsDescriptor keyFieldsDescriptor = keyFieldsDescriptors.get(index);
      Int2ObjectMap<FieldsDescriptor> map = ddIDToAggIDToInputAggDescriptor.get(index);
      IntArrayList aggIDList = ddIDToAggIDs.get(index);

      for(int aggIDIndex = 0;
          aggIDIndex < aggIDList.size();
          aggIDIndex++) {
        int aggID = aggIDList.get(aggIDIndex);
        events.add(createGenericAggregateEvent(inputEvent,
                                               eventSchema.getDdIDToDD().get(index),
                                               keyFieldsDescriptor,
                                               map.get(aggID),
                                               index,
                                               aggID));
      }
    }

    AggregateEvent[] gaes = new AggregateEvent[events.size()];

    for(int gaeIndex = 0;
        gaeIndex < events.size();
        gaeIndex++) {
      gaes[gaeIndex] = events.get(gaeIndex);
    }

    return gaes;
  }

  private AggregateEvent createGenericAggregateEvent(Map<String, Object> ga,
                                                            DimensionsDescriptor dd,
                                                            FieldsDescriptor keyFieldsDescriptor,
                                                            FieldsDescriptor aggregateDescriptor,
                                                            int dimensionDescriptorID,
                                                            int aggregateID)
  {
    GPOMutable keyGPO = new GPOMutable(keyFieldsDescriptor);

    List<String> fields = keyFieldsDescriptor.getFields().getFieldsList();

    for(int index = 0;
        index < fields.size();
        index++) {
      String field = fields.get(index);

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

    aggGPO.setField(JsonSalesGenerator.AGG_AMOUNT, ga.get(JsonSalesGenerator.AGG_AMOUNT));
    aggGPO.setField(JsonSalesGenerator.AGG_DISCOUNT, ga.get(JsonSalesGenerator.AGG_DISCOUNT));
    aggGPO.setField(JsonSalesGenerator.AGG_TAX, ga.get(JsonSalesGenerator.AGG_TAX));

    AggregateEvent gae = new AggregateEvent(new GPOImmutable(keyGPO),
                                                          aggGPO,
                                                          0,
                                                          dimensionDescriptorID,
                                                          aggregateID);
    return gae;
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
  public DimensionsAggregator getAggregator(int aggregatorID)
  {
    return AggregatorType.values()[aggregatorID].getAggregator();
  }

  @Override
  public FieldsDescriptor getAggregateFieldsDescriptor(int schemaID, int dimensionDescriptorID, int aggregatorID)
  {
    return ddIDToAggIDToOutputAggDescriptor.get(dimensionDescriptorID).get(aggregatorID);
  }

  @Override
  public DimensionsAggregator getAggregator(String aggregatorName)
  {
    return AggregatorType.NAME_TO_AGGREGATOR.get(aggregatorName);
  }

  @Override
  public Map<Integer, DimensionsAggregator> getAggregatorIDToAggregator()
  {
    return AggregatorType.ORDINAL_TO_AGGREGATOR;
  }
}
