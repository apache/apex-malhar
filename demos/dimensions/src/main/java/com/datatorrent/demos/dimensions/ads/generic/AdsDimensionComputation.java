/*
 *  Copyright (c) 2012-2015 Malhar, Inc.
 *  All Rights Reserved.
 */

package com.datatorrent.demos.dimensions.ads.generic;

import com.datatorrent.demos.dimensions.ads.schemas.AdsSchemaResult;
import com.datatorrent.lib.appdata.dimensions.AggregateEvent;
import com.datatorrent.lib.appdata.dimensions.DimensionsComputationSingleSchema;
import com.datatorrent.lib.appdata.dimensions.DimensionsDescriptor;
import com.datatorrent.lib.appdata.gpo.GPOImmutable;
import com.datatorrent.lib.appdata.gpo.GPOMutable;
import com.datatorrent.lib.appdata.schemas.FieldsDescriptor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

/**
 *
 * @author Timothy Farkas: tim@datatorrent.com
 */
public class AdsDimensionComputation extends DimensionsComputationSingleSchema<AdInfo>
{
  private static final Logger logger = LoggerFactory.getLogger(AdsDimensionComputation.class);

  public AdsDimensionComputation()
  {
  }

  @Override
  public AggregateEvent createGenericAggregateEvent(AdInfo ga,
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
}
