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

import org.codehaus.jettison.json.JSONArray;
import org.codehaus.jettison.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datatorrent.lib.appdata.gpo.GPOMutable;
import com.datatorrent.lib.appdata.gpo.GPOUtils;
import com.datatorrent.lib.appdata.schemas.DataQuerySnapshot;
import com.datatorrent.lib.appdata.schemas.DataResultSnapshot;
import com.datatorrent.lib.appdata.schemas.Message;
import com.datatorrent.lib.appdata.schemas.Result;
import com.datatorrent.lib.appdata.schemas.ResultFormatter;

/**
 * This class is used to serialize {@link DataResultSnapshot} objects.
 */
public class DataResultSnapshotSerializer implements CustomMessageSerializer
{
  /**
   * Constructor used to instantiate serializer in {@link MessageSerializerFactory}.
   */
  public DataResultSnapshotSerializer()
  {
  }

  @Override
  public String serialize(Message result, ResultFormatter resultFormatter)
  {
    try {
      return serializeHelper(result, resultFormatter);
    }
    catch(Exception ex) {
      throw new RuntimeException(ex);
    }
  }

  private String serializeHelper(Message result, ResultFormatter resultFormatter) throws Exception
  {
    DataResultSnapshot gResult = (DataResultSnapshot) result;

    JSONObject jo = new JSONObject();
    jo.put(Result.FIELD_ID, gResult.getId());
    jo.put(Result.FIELD_TYPE, gResult.getType());

    JSONArray ja = new JSONArray();

    for(GPOMutable value: gResult.getValues()) {
      JSONObject dataValue = GPOUtils.serializeJSONObject(value,
                                                          ((DataQuerySnapshot) gResult.getQuery()).getFields(),
                                                          resultFormatter);
      ja.put(dataValue);
    }

    jo.put(DataResultSnapshot.FIELD_DATA, ja);

    if(!gResult.isOneTime()) {
      jo.put(Result.FIELD_COUNTDOWN, gResult.getCountdown());
    }

    return jo.toString();
  }

  private static final Logger LOG = LoggerFactory.getLogger(DataResultSnapshotSerializer.class);
}
