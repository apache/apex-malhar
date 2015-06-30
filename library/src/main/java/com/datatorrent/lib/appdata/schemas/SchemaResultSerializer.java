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
package com.datatorrent.lib.appdata.schemas;

import org.codehaus.jettison.json.JSONArray;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datatorrent.lib.appdata.query.serde.CustomMessageSerializer;

/**
 * This is the serializer class for {@link SchemaResult} objects.
 */
public class SchemaResultSerializer implements CustomMessageSerializer
{
  /**
   * This constructor is required by the {@link CustomMessageSerializer} interface.
   */
  public SchemaResultSerializer()
  {
    //Do nothing
  }

  @Override
  public String serialize(Message message, ResultFormatter resultFormatter)
  {
    if(!(message instanceof SchemaResult))
    {
      throw new IllegalArgumentException("Must receive a "
                                         + SchemaResult.class
                                         + " object.");
    }

    SchemaResult genericSchemaResult = (SchemaResult) message;

    StringBuilder sb = new StringBuilder();

    logger.debug("result {}", genericSchemaResult);
    logger.debug("result id {}", genericSchemaResult.getId());
    logger.debug("result type {}", genericSchemaResult.getType());

    sb.append("{\"").append(Result.FIELD_ID).
    append("\":\"").append(genericSchemaResult.getId()).
    append("\",\"").append(Result.FIELD_TYPE).
    append("\":\"").append(genericSchemaResult.getType()).
    append("\",\"").append(Result.FIELD_DATA).
    append("\":");

    JSONArray schemaArray = new JSONArray();

    for(Schema schema: genericSchemaResult.getGenericSchemas()) {
      try {
        schemaArray.put(new JSONObject(schema.getSchemaJSON()));
      }
      catch(JSONException ex) {
        throw new RuntimeException(ex);
      }
    }

    sb.append(schemaArray.toString()).append("}");

    return sb.toString();
  }

  private static final Logger logger = LoggerFactory.getLogger(SchemaResultSerializer.class);
}
