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
import com.datatorrent.lib.converter.Converter;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.codehaus.jettison.json.JSONArray;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;

import java.util.Iterator;
import java.util.List;
import java.util.Map;


/**
 *
 * {
 *    "gpoField":["pojoField1","pojoField2"];
 * }
 *
 */
public class DimensionsPOJOConverter implements Converter<Object, AggregateEvent, DimensionConversionContext>
{
  private String pojoMappingSchema;
  private Map<String, List<String>> gpoFieldToPojoGetters = Maps.newHashMap();

  public DimensionsPOJOConverter()
  {
  }

  public void setPojoMappingSchema(String pojoMappingSchema)
  {
    this.pojoMappingSchema = Preconditions.checkNotNull(pojoMappingSchema);

    JSONObject jo = null;

    try {
      jo = new JSONObject(pojoMappingSchema);
    }
    catch(JSONException ex) {
      throw new RuntimeException(ex);
    }

    Iterator keyIterator = jo.keys();

    while(keyIterator.hasNext()) {
      String gpoField = (String) keyIterator.next();
      JSONArray pojoGettersArray = null;

      try {
        pojoGettersArray = jo.getJSONArray(gpoField);
      }
      catch(JSONException ex) {
        throw new RuntimeException(ex);
      }

      Preconditions.checkArgument(pojoGettersArray.length() > 0,
                                  "pojo getters array cannot be empty");

      List<String> getters = Lists.newArrayList();

      for(int getterIndex = 0;
          getterIndex < pojoGettersArray.length();
          getterIndex++) {
        try {
          getters.add(pojoGettersArray.getString(getterIndex));
        }
        catch(JSONException ex) {
          throw new RuntimeException(ex);
        }
      }

      gpoFieldToPojoGetters.put(gpoField, getters);
    }
  }

  public String getPojoMappingSchema()
  {
    return pojoMappingSchema;
  }

  @Override
  public AggregateEvent convert(Object inputEvent, DimensionConversionContext context)
  {
    throw new UnsupportedOperationException("This is not implemented yet.");
  }
}
