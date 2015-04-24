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

package com.datatorrent.lib.appdata.dimensions;

import com.datatorrent.lib.appdata.converter.MapGPOConverterSchema;
import com.datatorrent.lib.appdata.dimensions.AggregateEvent;
import com.datatorrent.lib.appdata.dimensions.DimensionsDescriptor;
import com.datatorrent.lib.appdata.gpo.GPOMutable;
import com.datatorrent.lib.converter.Converter;
import com.google.common.collect.Maps;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;

/**
 * {
 * "gpoField":"mapField"
 * }
 */
public class DimensionsMapConverter implements Converter<Map<String, Object>, AggregateEvent, DimensionsConversionContext>
{
  private static final Logger logger = LoggerFactory.getLogger(DimensionsMapConverter.class);

  private MapGPOConverterSchema converterSchema;
  private Map<String, String> gpoFieldToMapField = Maps.newHashMap();

  public DimensionsMapConverter()
  {
  }

  public void setConversionSchema(String conversionSchema)
  {
    converterSchema = new MapGPOConverterSchema(conversionSchema);
  }

  public String getConversionSchema()
  {
    return converterSchema.getJsonString();
  }

  @Override
  public AggregateEvent convert(Map<String, Object> inputEvent, DimensionsConversionContext context)
  {
    GPOMutable key = new GPOMutable(context.keyFieldsDescriptor);

    List<String> fields = key.getFieldDescriptor().getFields().getFieldsList();

    for(int fieldIndex = 0;
        fieldIndex < fields.size();
        fieldIndex++) {
      String field = fields.get(fieldIndex);
      if(field.equals(DimensionsDescriptor.DIMENSION_TIME_BUCKET)) {
      }
      else if(field.equals(DimensionsDescriptor.DIMENSION_TIME)) {
        long timestamp = (Long) inputEvent.get(converterSchema.getMapField(field));
        context.dd.getTimeBucket().roundDown(timestamp);
        key.setField(field, timestamp);
      }
      else {
        key.setField(field, inputEvent.get(converterSchema.getMapField(field)));
      }
    }

    GPOMutable aggregates = new GPOMutable(context.aggregateDescriptor);

    fields = aggregates.getFieldDescriptor().getFields().getFieldsList();

    for(int fieldIndex = 0;
        fieldIndex < fields.size();
        fieldIndex++) {
      String field = fields.get(fieldIndex);
      aggregates.setField(field, inputEvent.get(converterSchema.getMapField(field)));
    }

    return new AggregateEvent(new GPOMutable(key),
                              aggregates,
                              context.schemaID,
                              context.dimensionDescriptorID,
                              context.aggregatorID);
  }
}
