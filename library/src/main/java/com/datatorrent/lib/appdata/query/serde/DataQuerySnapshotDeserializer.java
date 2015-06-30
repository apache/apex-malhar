/*
 * Copyright (c) 2015 DataTorrent, Inc.
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
package com.datatorrent.lib.appdata.query.serde;

import java.io.IOException;

import java.util.Map;
import java.util.Set;

import com.google.common.collect.Sets;

import org.codehaus.jettison.json.JSONArray;
import org.codehaus.jettison.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datatorrent.lib.appdata.schemas.DataQuerySnapshot;
import com.datatorrent.lib.appdata.schemas.Fields;
import com.datatorrent.lib.appdata.schemas.Message;
import com.datatorrent.lib.appdata.schemas.QRBase;
import com.datatorrent.lib.appdata.schemas.Query;
import com.datatorrent.lib.appdata.schemas.SchemaUtils;

/**
 * This class is a deserializer for {@link DataQuerySnapshot} objects.
 */
public class DataQuerySnapshotDeserializer implements CustomMessageDeserializer
{
  /**
   * Constructor used to instantiate deserializer in {@link MessageDeserializerFactory}.
   */
  public DataQuerySnapshotDeserializer()
  {
  }

  @Override
  public Message deserialize(String json,
                             Class<? extends Message> clazz,
                             Object context) throws IOException
  {
    try {
      return deserializeHelper(json,
                               context);
    }
    catch(Exception ex) {
      throw new IOException(ex);
    }
  }

  /**
   * This is a helper deserializer method.
   * @param json The JSON to deserialize.
   * @param context The context information to use when deserializing the query.
   * @return The deserialized query. If the given json contains some invalid content this
   * method may return null.
   * @throws Exception
   */
  private Message deserializeHelper(String json,
                                    Object context) throws Exception
  {
    JSONObject jo = new JSONObject(json);

    //// Query id stuff
    String id = jo.getString(QRBase.FIELD_ID);
    String type = jo.getString(Message.FIELD_TYPE);

    if(!type.equals(DataQuerySnapshot.TYPE)) {
      LOG.error("Found type {} in the query json, but expected type {}.", type, DataQuerySnapshot.TYPE);
      return null;
    }

    /// Countdown
    long countdown = -1L;
    boolean hasCountdown = jo.has(QRBase.FIELD_COUNTDOWN);

    if(hasCountdown) {
      countdown = jo.getLong(QRBase.FIELD_COUNTDOWN);
    }

    ////Data
    Map<String, String> schemaKeys = null;
    Set<String> fieldsSet = Sets.newHashSet();

    if(jo.has(DataQuerySnapshot.FIELD_DATA)) {
      JSONObject data = jo.getJSONObject(DataQuerySnapshot.FIELD_DATA);

      if(data.has(Query.FIELD_SCHEMA_KEYS)) {
        schemaKeys = SchemaUtils.extractMap(data.getJSONObject(Query.FIELD_SCHEMA_KEYS));
      }

      if(data.has(DataQuerySnapshot.FIELD_FIELDS)) {
        //// Fields
        JSONArray jArray = data.getJSONArray(DataQuerySnapshot.FIELD_FIELDS);

        for(int index = 0;
            index < jArray.length();
            index++) {
          String field = jArray.getString(index);

          if(!fieldsSet.add(field)) {
            LOG.error("The field {} was listed more than once, this is an invalid query.", field);
          }
        }
      }
    }

    Fields fields = new Fields(fieldsSet);

    if(!hasCountdown) {
      return new DataQuerySnapshot(id,
                                  fields,
                                  schemaKeys);
    }
    else {
      return new DataQuerySnapshot(id,
                                  fields,
                                  countdown,
                                  schemaKeys);
    }
  }

  private static final Logger LOG = LoggerFactory.getLogger(DataQuerySnapshotDeserializer.class);
}
