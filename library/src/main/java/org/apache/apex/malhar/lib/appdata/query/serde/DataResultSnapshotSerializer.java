/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.apex.malhar.lib.appdata.query.serde;

import org.codehaus.jettison.json.JSONArray;
import org.codehaus.jettison.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.apex.malhar.lib.appdata.gpo.GPOMutable;
import org.apache.apex.malhar.lib.appdata.gpo.GPOUtils;
import org.apache.apex.malhar.lib.appdata.schemas.DataQuerySnapshot;
import org.apache.apex.malhar.lib.appdata.schemas.DataResultSnapshot;
import org.apache.apex.malhar.lib.appdata.schemas.Message;
import org.apache.apex.malhar.lib.appdata.schemas.Result;
import org.apache.apex.malhar.lib.appdata.schemas.ResultFormatter;

/**
 * This class is used to serialize {@link DataResultSnapshot} objects.
 * @since 3.0.0
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
    } catch (Exception ex) {
      throw new RuntimeException(ex);
    }
  }

  private String serializeHelper(Message result, ResultFormatter resultFormatter) throws Exception
  {
    DataResultSnapshot gResult = (DataResultSnapshot)result;

    JSONObject jo = new JSONObject();
    jo.put(Result.FIELD_ID, gResult.getId());
    jo.put(Result.FIELD_TYPE, gResult.getType());

    JSONArray ja = new JSONArray();

    for (GPOMutable value : gResult.getValues()) {
      JSONObject dataValue = GPOUtils.serializeJSONObject(value, ((DataQuerySnapshot)gResult.getQuery()).getFields(),
          resultFormatter);
      ja.put(dataValue);
    }

    jo.put(DataResultSnapshot.FIELD_DATA, ja);

    if (!gResult.isOneTime()) {
      jo.put(Result.FIELD_COUNTDOWN, gResult.getCountdown());
    }

    return jo.toString();
  }

  private static final Logger LOG = LoggerFactory.getLogger(DataResultSnapshotSerializer.class);
}
