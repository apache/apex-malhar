/*
 *  Copyright (c) 2012-2015 Malhar, Inc.
 *  All Rights Reserved.
 */

package com.datatorrent.demos.dimensions.sales.generic;

import com.datatorrent.lib.appdata.dimensions.AggregateEvent;
import com.datatorrent.lib.appdata.dimensions.DimensionsComputationSingleSchema;
import com.datatorrent.lib.appdata.dimensions.DimensionsDescriptor;
import com.datatorrent.lib.appdata.gpo.GPOImmutable;
import com.datatorrent.lib.appdata.gpo.GPOMutable;
import com.datatorrent.lib.appdata.schemas.FieldsDescriptor;

import java.util.List;
import java.util.Map;

/**
 *
 * @author Timothy Farkas: tim@datatorrent.com
 */
public class SalesDimensionComputation extends DimensionsComputationSingleSchema<Map<String,Object>>
{
  public SalesDimensionComputation()
  {
  }

  public AggregateEvent createGenericAggregateEvent(Map<String, Object> ga,
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
}
