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
package com.datatorrent.lib.appdata.query;

import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;

import com.datatorrent.lib.appdata.query.serde.CustomMessageSerializer;
import com.datatorrent.lib.appdata.schemas.Message;
import com.datatorrent.lib.appdata.schemas.Result;
import com.datatorrent.lib.appdata.schemas.ResultFormatter;

public class MockResultSerializer implements CustomMessageSerializer
{
  public MockResultSerializer()
  {
  }

  @Override
  public String serialize(Message message, ResultFormatter resultFormatter)
  {
    try {
      return serializeHelper(message, resultFormatter);
    }
    catch(JSONException e) {
      throw new RuntimeException(e);
    }
  }

  private String serializeHelper(Message message, ResultFormatter resultFormatter) throws JSONException
  {
    MockResult result = (MockResult) message;

    JSONObject jo = new JSONObject();

    jo.put(Result.FIELD_ID, result.getId());
    jo.put(Result.FIELD_TYPE, result.getType());

    return jo.toString();
  }
}
