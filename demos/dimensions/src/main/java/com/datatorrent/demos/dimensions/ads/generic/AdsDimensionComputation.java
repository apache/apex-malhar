/*
 *  Copyright (c) 2012-2015 Malhar, Inc.
 *  All Rights Reserved.
 */

package com.datatorrent.demos.dimensions.ads.generic;

import com.datatorrent.api.Context.OperatorContext;
import com.datatorrent.demos.dimensions.ads.schemas.AdsSchemaResult;
import com.datatorrent.lib.appdata.dimensions.AggregateEvent;
import com.datatorrent.lib.appdata.dimensions.AggregatorStaticType;
import com.datatorrent.lib.appdata.dimensions.AggregatorUtils;
import com.datatorrent.lib.appdata.dimensions.DimensionsComputation;
import com.datatorrent.lib.appdata.dimensions.DimensionsDescriptor;
import com.datatorrent.lib.appdata.dimensions.DimensionsStaticAggregator;
import com.datatorrent.lib.appdata.gpo.GPOImmutable;
import com.datatorrent.lib.appdata.gpo.GPOMutable;
import com.datatorrent.lib.appdata.schemas.DimensionalEventSchema;
import com.datatorrent.lib.appdata.schemas.FieldsDescriptor;
import com.google.common.collect.Lists;
import it.unimi.dsi.fastutil.ints.Int2ObjectMap;
import it.unimi.dsi.fastutil.ints.IntArrayList;
import javax.validation.constraints.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;

/**
 *
 * @author Timothy Farkas: tim@datatorrent.com
 */
public class AdsDimensionComputation extends DimensionsComputation<AdInfo>
{
  private static final Logger logger = LoggerFactory.getLogger(AdsDimensionComputation.class);

  @NotNull
  private String eventSchemaJSON;

  private transient DimensionalEventSchema eventSchema;

  public AdsDimensionComputation()
  {
  }

  @Override
  public void setup(OperatorContext context)
  {
    eventSchema = new DimensionalEventSchema(eventSchemaJSON,
                                             AggregatorUtils.DEFAULT_AGGREGATOR_INFO);

    super.setup(context);
  }

  @Override
  public AggregateEvent[] convertInputEvent(AdInfo inputEvent)
  {
    List<AggregateEvent> events = Lists.newArrayList();
    List<FieldsDescriptor> keyFieldsDescriptors = eventSchema.getDdIDToKeyDescriptor();

    for(int index = 0;
        index < keyFieldsDescriptors.size();
        index++) {
      FieldsDescriptor keyFieldsDescriptor = keyFieldsDescriptors.get(index);
      Int2ObjectMap<FieldsDescriptor> map = eventSchema.getDdIDToAggIDToInputAggDescriptor().get(index);
      IntArrayList aggIDList = eventSchema.getDdIDToAggIDs().get(index);

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

  private AggregateEvent createGenericAggregateEvent(AdInfo ga,
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
        if(dd.getTimeBucket() == null) {
          keyGPO.setField(field, ga.getTime());
        }
        else {
          keyGPO.setField(field, dd.getTimeBucket().roundDown(ga.getTime()));
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

    fields = aggregateDescriptor.getFields().getFieldsList();

    for(int index = 0;
        index < fields.size();
        index++) {
      String field = fields.get(index);

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

    AggregateEvent gae = new AggregateEvent(new GPOImmutable(keyGPO),
                                                          aggGPO,
                                                          0,
                                                          dimensionDescriptorID,
                                                          aggregateID);
    return gae;
  }

  @Override
  public DimensionsStaticAggregator getAggregator(int aggregatorID)
  {
    return AggregatorStaticType.values()[aggregatorID].getAggregator();
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
    return eventSchema.getDdIDToAggIDToOutputAggDescriptor().get(dimensionDescriptorID).get(aggregatorID);
  }

  @Override
  public DimensionsStaticAggregator getAggregator(String aggregatorName)
  {
    return AggregatorStaticType.NAME_TO_AGGREGATOR.get(aggregatorName);
  }

  @Override
  public Map<Integer, DimensionsStaticAggregator> getAggregatorIDToAggregator()
  {
    return AggregatorStaticType.ORDINAL_TO_AGGREGATOR;
  }
}
