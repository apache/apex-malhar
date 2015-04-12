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

import com.datatorrent.lib.appdata.gpo.GPOMutable;
import com.datatorrent.lib.appdata.gpo.GPOUtils;
import com.datatorrent.lib.appdata.qr.CustomDataSerializer;
import com.datatorrent.lib.appdata.qr.Result;
import org.codehaus.jettison.json.JSONArray;
import org.codehaus.jettison.json.JSONObject;

public class DataResultTabularSerializer implements CustomDataSerializer
{
  public DataResultTabularSerializer()
  {
  }

  @Override
  public String serialize(Result result)
  {
    try {
      return serializeHelper(result);
    }
    catch(Exception ex) {
      throw new RuntimeException(ex);
    }
  }

  private String serializeHelper(Result result) throws Exception
  {
    DataResultTabular gResult = (DataResultTabular) result;

    JSONObject jo = new JSONObject();
    jo.put(Result.FIELD_ID, gResult.getId());
    jo.put(Result.FIELD_TYPE, gResult.getType());

    JSONArray ja = new JSONArray();

    for(GPOMutable value: gResult.getValues()) {
      JSONObject dataValue = GPOUtils.serializeJSONObject(value, ((DataQueryTabular) gResult.getQuery()).getFields());
      ja.put(dataValue);
    }

    jo.put(DataResultTabular.FIELD_DATA, ja);

    return jo.toString();
  }
}
