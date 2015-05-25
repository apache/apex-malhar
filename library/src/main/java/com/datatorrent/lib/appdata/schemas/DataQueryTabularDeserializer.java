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

import com.datatorrent.lib.appdata.query.serde.CustomMessageDeserializer;
import com.datatorrent.lib.appdata.query.serde.Message;
import com.datatorrent.lib.appdata.query.serde.Query;
import com.google.common.collect.Sets;
import java.io.IOException;
import org.codehaus.jettison.json.JSONArray;
import org.codehaus.jettison.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.Set;

public class DataQueryTabularDeserializer extends CustomMessageDeserializer
{
  private static final Logger logger = LoggerFactory.getLogger(DataQueryTabularDeserializer.class);

  @Override
  public Message deserialize(String json, Object context) throws IOException
  {
    try {
      return deserializeHelper(json,
                               context);
    }
    catch(Exception ex) {
      throw new IOException(ex);
    }
  }

  private Message deserializeHelper(String json,
                                 Object context) throws Exception
  {
    JSONObject jo = new JSONObject(json);

    //// Query id stuff
    String id = jo.getString(Query.FIELD_ID);
    String type = jo.getString(Message.FIELD_TYPE);

    /// Countdown
    long countdown = -1L;
    boolean hasCountdown = jo.has(DataQueryTabular.FIELD_COUNTDOWN);

    if(hasCountdown) {
      countdown = jo.getLong(DataQueryTabular.FIELD_COUNTDOWN);
    }

    ////Data
    Map<String, String> schemaKeys = null;
    Set<String> fieldsSet = Sets.newHashSet();

    if(jo.has(DataQueryTabular.FIELD_DATA)) {
      JSONObject data = jo.getJSONObject(DataQueryTabular.FIELD_DATA);

      if(data.has(Query.FIELD_SCHEMA_KEYS)) {
        schemaKeys = SchemaUtils.extractMap(data.getJSONObject(Query.FIELD_SCHEMA_KEYS));
      }

      if(data.has(DataQueryTabular.FIELD_FIELDS)) {
        //// Fields
        JSONArray jArray = data.getJSONArray(DataQueryTabular.FIELD_FIELDS);

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
      return new DataQueryTabular(id,
                                  type,
                                  fields,
                                  schemaKeys);
    }
    else {
      return new DataQueryTabular(id,
                                  type,
                                  fields,
                                  countdown,
                                  schemaKeys);
    }
  }
}
