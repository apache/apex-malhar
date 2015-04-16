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

import com.datatorrent.lib.appdata.qr.CustomDataSerializer;
import com.datatorrent.lib.appdata.qr.Result;
import org.codehaus.jettison.json.JSONArray;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;


public class SchemaSerializer implements CustomDataSerializer
{
  public SchemaSerializer()
  {
    //Do nothing
  }

  @Override
  public String serialize(Result result)
  {
    if(!(result instanceof SchemaResult))
    {
      throw new IllegalArgumentException("Must receive a "
                                         + SchemaResult.class
                                         + " object.");
    }

    SchemaResult genericSchemaResult = (SchemaResult) result;

    StringBuilder sb = new StringBuilder();

    sb.append("{\"").append(Result.FIELD_ID).
    append("\":\"").append(result.getId()).
    append("\",\"").append(Result.FIELD_TYPE).
    append("\":\"").append(result.getType()).
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
}
