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
package com.datatorrent.lib.appdata.schemas;

import com.datatorrent.lib.appdata.dimensions.DimensionsDescriptor;
import com.datatorrent.lib.appdata.gpo.GPOMutable;
import com.datatorrent.lib.appdata.qr.CustomDataSerializer;
import com.datatorrent.lib.appdata.qr.Result;
import org.codehaus.jettison.json.JSONArray;
import org.codehaus.jettison.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.Set;

public class DataResultDimensionalSerializer implements CustomDataSerializer
{
  public static final String ALL = "ALL";

  private static final Logger logger = LoggerFactory.getLogger(DataResultDimensionalSerializer.class);

  public DataResultDimensionalSerializer()
  {
  }

  @Override
  public String serialize(Result result, AppDataFormatter appDataFormatter)
  {
    try {
      return serializeHelper(result, appDataFormatter);
    }
    catch(Exception e) {
      throw new RuntimeException(e);
    }
  }

  private String serializeHelper(Result result, AppDataFormatter appDataFormatter) throws Exception
  {
    DataResultDimensional dataResult = (DataResultDimensional) result;

    JSONObject jo = new JSONObject();

    jo.put(Result.FIELD_ID, dataResult.getId());
    jo.put(Result.FIELD_TYPE, dataResult.getType());

    JSONArray data = new JSONArray();
    jo.put(Result.FIELD_DATA, data);

    //dataResult.getQuery().g

    boolean hasTime = dataResult.getQuery().isHasTime();

    FieldsAggregatable fieldsAggregatable = dataResult.getQuery().getFieldsAggregatable();
    Fields nonAggregatedFields = fieldsAggregatable.getNonAggregatedFields();
    Map<String, Set<String>> aggregatorToFields = fieldsAggregatable.getAggregatorToFields();
    Map<String, Map<String, String>> aggregatorToFieldToName = fieldsAggregatable.getAggregatorToFieldToName();

    List<Map<String, GPOMutable>> keys = dataResult.getKeys();
    List<Map<String, GPOMutable>> values = dataResult.getValues();

    for(int index = 0;
        index < keys.size();
        index++) {
      Map<String, GPOMutable> key = keys.get(index);
      Map<String, GPOMutable> value = values.get(index);

      JSONObject valueJO = new JSONObject();

      GPOMutable gpoKey = key.values().iterator().next();

      if(hasTime && nonAggregatedFields.getFields().contains(DimensionsDescriptor.DIMENSION_TIME)) {
        Object time = gpoKey.getField(DimensionsDescriptor.DIMENSION_TIME);
        valueJO.put(DimensionsDescriptor.DIMENSION_TIME, time);
      }

      for(String field: nonAggregatedFields.getFields()) {
        Type type = fieldsAggregatable.getFieldsDescriptor().getType(field);

        if(field.equals(DimensionsDescriptor.DIMENSION_TIME)) {
          //Do nothing
        }
        else if(gpoKey.getFieldDescriptor().getFields().getFields().contains(field)) {
          valueJO.put(field, appDataFormatter.format(gpoKey.getField(field)));
        }
        else {
          valueJO.put(field, ALL);
        }
      }

      for(Map.Entry<String, GPOMutable> entry: value.entrySet()) {
        String aggregatorName = entry.getKey();
        GPOMutable aggregateValues = entry.getValue();
        Set<String> fields = aggregatorToFields.get(aggregatorName);

        for(String field: fields) {
          String compoundName = aggregatorToFieldToName.get(aggregatorName).get(field);
          valueJO.put(compoundName, appDataFormatter.format(aggregateValues.getField(field)));
        }
      }

      data.put(valueJO);
    }

    if(!dataResult.getQuery().isOneTime()) {
      jo.put(DataResultDimensional.FIELD_COUNTDOWN,
             dataResult.getCountdown());
    }

    return jo.toString();
  }
}
