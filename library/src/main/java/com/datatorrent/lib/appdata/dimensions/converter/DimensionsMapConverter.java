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

package com.datatorrent.lib.appdata.dimensions.converter;

import com.datatorrent.lib.appdata.dimensions.AggregateEvent;
import com.datatorrent.lib.appdata.gpo.GPOImmutable;
import com.datatorrent.lib.appdata.gpo.GPOMutable;
import com.datatorrent.lib.converter.Converter;
import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;

import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 * {
 * "gpoField":"mapField"
 * }
 */
public class DimensionsMapConverter implements Converter<Map<String, Object>, AggregateEvent, DimensionsConversionContext>
{
  private String conversionSchema;
  private Map<String, String> gpoFieldToMapField = Maps.newHashMap();

  public DimensionsMapConverter()
  {
  }

  public void setConversionSchema(String conversionSchema)
  {
    this.conversionSchema = Preconditions.checkNotNull(conversionSchema);
    JSONObject jo = null;

    try {
      jo = new JSONObject(conversionSchema);
    }
    catch(JSONException ex) {
      throw new RuntimeException(ex);
    }

    Iterator keyIterator = jo.keys();

    while(keyIterator.hasNext()) {
      String key = (String) keyIterator.next();
      String value = null;

      try {
        value = jo.getString((String) keyIterator.next());
      }
      catch(JSONException ex) {
        throw new IllegalArgumentException(ex);
      }

      gpoFieldToMapField.put(key, value);
    }
  }

  public String getConversionSchema()
  {
    return conversionSchema;
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
      key.setField(field, inputEvent.get(getMapField(field)));
    }

    GPOMutable aggregates = new GPOMutable(context.aggregateDescriptor);

    fields = aggregates.getFieldDescriptor().getFields().getFieldsList();

    for(int fieldIndex = 0;
        fieldIndex < fields.size();
        fieldIndex++) {
      String field = fields.get(fieldIndex);
      aggregates.setField(field, inputEvent.get(getMapField(field)));
    }

    return new AggregateEvent(new GPOImmutable(key),
                              aggregates,
                              context.schemaID,
                              context.dimensionDescriptorID,
                              context.aggregatorID);
  }

  private String getMapField(String gpoField)
  {
    String mapField = gpoFieldToMapField.get(gpoField);

    if(mapField == null) {
      return gpoField;
    }

    return mapField;
  }
}
