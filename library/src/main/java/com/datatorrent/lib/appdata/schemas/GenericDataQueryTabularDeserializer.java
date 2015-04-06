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

import com.datatorrent.lib.appdata.qr.CustomDataDeserializer;
import com.datatorrent.lib.appdata.qr.Data;
import com.datatorrent.lib.appdata.qr.Query;
import com.google.common.collect.Sets;
import org.codehaus.jettison.json.JSONArray;
import org.codehaus.jettison.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Set;

public class GenericDataQueryTabularDeserializer extends CustomDataDeserializer
{
  private static final Logger logger = LoggerFactory.getLogger(GenericDataQueryTabularDeserializer.class);

  @Override
  public Data deserialize(String json, Object context)
  {
    try {
      return deserializeHelper(json,
                               context);
    }
    catch(Exception ex) {
      logger.error("An error happened while deserializing query:", ex);
    }

    return null;
  }

  private Data deserializeHelper(String json,
                                 Object context) throws Exception
  {
    GenericSchemaTabular schema = (GenericSchemaTabular) context;

    JSONObject jo = new JSONObject(json);

    //// Query id stuff
    String id = jo.getString(Query.FIELD_ID);
    String type = jo.getString(Data.FIELD_TYPE);

    /// Countdown
    long countdown = -1L;
    boolean hasCountdown = jo.has(GenericDataQueryTabular.FIELD_COUNTDOWN);

    if(hasCountdown) {
      countdown = jo.getLong(GenericDataQueryTabular.FIELD_COUNTDOWN);
    }

    ////Data

    Set<String> fieldsSet = Sets.newHashSet();

    if(jo.has(GenericDataQueryTabular.FIELD_DATA)) {
      JSONObject data = jo.getJSONObject(GenericDataQueryTabular.FIELD_DATA);
      if(data.has(GenericDataQueryTabular.FIELD_FIELDS)) {
        //// Fields
        JSONArray jArray = data.getJSONArray(GenericDataQueryTabular.FIELD_FIELDS);

        for(int index = 0;
            index < jArray.length();
            index++) {
          String field = jArray.getString(index);

          if(!fieldsSet.add(field)) {
            logger.error("The field {} was listed more than once, this is an invalid query.", field);
          }
        }
      }
    }

    Fields fields = new Fields(fieldsSet);

    if(!hasCountdown) {
      return new GenericDataQueryTabular(id,
                                         type,
                                         fields);
    }
    else {
      return new GenericDataQueryTabular(id,
                                         type,
                                         fields,
                                         countdown);
    }
  }
}
