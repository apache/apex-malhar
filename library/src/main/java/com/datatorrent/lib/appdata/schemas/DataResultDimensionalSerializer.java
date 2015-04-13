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
  private static final Logger logger = LoggerFactory.getLogger(DataResultDimensionalSerializer.class);

  public DataResultDimensionalSerializer()
  {
  }

  @Override
  public String serialize(Result result)
  {
    try {
      return serializeHelper(result);
    }
    catch(Exception e) {
      throw new RuntimeException(e);
    }
  }

  private String serializeHelper(Result result) throws Exception
  {
    DataResultDimensional dataResult = (DataResultDimensional) result;

    JSONObject jo = new JSONObject();

    jo.put(Result.FIELD_ID, dataResult.getId());
    jo.put(Result.FIELD_TYPE, dataResult.getType());

    JSONArray data = new JSONArray();
    jo.put(Result.FIELD_DATA, data);

    //dataResult.getQuery().g

    boolean hasTime = dataResult.getQuery().isHasTime();

    logger.info("Serializing result hasTime {}", hasTime);

    FieldsAggregatable fieldsAggregatable = dataResult.getQuery().getFieldsAggregatable();
    Map<String, Set<String>> aggregatorToFields = fieldsAggregatable.getAggregatorToFields();
    Map<String, Map<String, String>> aggregatorToFieldToName = fieldsAggregatable.getAggregatorToFieldToName();

    List<Map<String, GPOMutable>> keys = dataResult.getKeys();
    List<Map<String, GPOMutable>> values = dataResult.getValues();

    for(int index = 0;
        index < keys.size();
        index++) {
      Map<String, GPOMutable> key = keys.get(index);
      Map<String, GPOMutable> value = values.get(index);

      Object time = key.get(DimensionsDescriptor.DIMENSION_TIME);
      JSONObject valueJO = new JSONObject();

      if(hasTime) {
        logger.info("Adding time to result {}", time);
        valueJO.put(DimensionsDescriptor.DIMENSION_TIME, time);
      }

      for(Map.Entry<String, GPOMutable> entry: value.entrySet()) {
        String aggregatorName = entry.getKey();
        GPOMutable aggregateValues = entry.getValue();
        Set<String> fields = aggregatorToFields.get(aggregatorName);

        for(String field: fields) {
          String compoundName = aggregatorToFieldToName.get(aggregatorName).get(field);
          valueJO.put(compoundName, aggregateValues.getField(field));
        }
      }

      logger.info("valueJO {}", valueJO.toString());
      data.put(valueJO);
    }

    if(!dataResult.getQuery().isOneTime()) {
      jo.put(DataResultDimensional.FIELD_COUNTDOWN,
             dataResult.getCountdown());
    }

    return jo.toString();
  }
}
