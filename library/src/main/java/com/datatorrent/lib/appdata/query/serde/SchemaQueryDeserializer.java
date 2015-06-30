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

import org.codehaus.jettison.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datatorrent.lib.appdata.schemas.Message;
import com.datatorrent.lib.appdata.schemas.Query;
import com.datatorrent.lib.appdata.schemas.SchemaQuery;
import com.datatorrent.lib.appdata.schemas.SchemaUtils;

public class SchemaQueryDeserializer implements CustomMessageDeserializer
{
  @Override
  public Message deserialize(String json, Class<? extends Message> message, Object context) throws IOException
  {
    try {
      return deserializeHelper(json, message, context);
    }
    catch(Exception ex) {
      throw new RuntimeException(ex);
    }
  }

  private Message deserializeHelper(String json, Class<? extends Message> message, Object context) throws Exception
  {
    JSONObject schemaJO = new JSONObject(json);

    String type = schemaJO.getString(Query.FIELD_TYPE);

    if(!type.equals(SchemaQuery.TYPE)) {
      LOG.error("The given type {} is invalid.", type);
      return null;
    }

    String id = schemaJO.getString(Query.FIELD_ID);
    Map<String, String> contextKeysMap = null;
    Map<String, String> schemaKeysMap = null;

    if(schemaJO.has(SchemaQuery.FIELD_CONTEXT)) {
      JSONObject contextJO = schemaJO.getJSONObject(SchemaQuery.FIELD_CONTEXT);

      if(contextJO.length() == 0) {
        LOG.error("The context cannot be empty");
        return null;
      }

      if(contextJO.has(SchemaQuery.FIELD_CONTEXT_KEYS)) {
        JSONObject keys = contextJO.getJSONObject(SchemaQuery.FIELD_CONTEXT_KEYS);
        contextKeysMap = SchemaUtils.extractMap(keys);

        if(contextKeysMap.isEmpty()) {
          contextKeysMap = null;
        }
      }

      if(contextJO.has(SchemaQuery.FIELD_SCHEMA_KEYS)) {
        JSONObject schemaKeys = contextJO.getJSONObject(SchemaQuery.FIELD_SCHEMA_KEYS);
        schemaKeysMap = SchemaUtils.extractMap(schemaKeys);

        if(schemaKeysMap.isEmpty()) {
          schemaKeysMap = null;
        }
      }
    }

    SchemaQuery sq = new SchemaQuery(id,
                           schemaKeysMap,
                           contextKeysMap);

    return sq;
  }

  private static final Logger LOG = LoggerFactory.getLogger(SchemaQueryDeserializer.class);
}
