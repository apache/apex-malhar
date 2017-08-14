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

import java.io.IOException;
import java.util.Map;
import java.util.Set;

import org.codehaus.jettison.json.JSONArray;
import org.codehaus.jettison.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.apex.malhar.lib.appdata.schemas.DataQuerySnapshot;
import org.apache.apex.malhar.lib.appdata.schemas.Fields;
import org.apache.apex.malhar.lib.appdata.schemas.Message;
import org.apache.apex.malhar.lib.appdata.schemas.SchemaUtils;

import com.google.common.collect.Sets;

/**
 * This class is a deserializer for {@link DataQuerySnapshot} objects.
 * @since 3.0.0
 */
public class DataQuerySnapshotDeserializer implements CustomMessageDeserializer
{
  public static final Set<Fields> FIRST_LEVEL_FIELD_COMBINATIONS;
  public static final Set<Fields> DATA_FIELD_COMBINATIONS;

  static {
    Set<Fields> firstLevelFieldCombinations = Sets.newHashSet();
    firstLevelFieldCombinations.add(new Fields(Sets.newHashSet(DataQuerySnapshot.FIELD_ID, DataQuerySnapshot.FIELD_TYPE,
        DataQuerySnapshot.FIELD_COUNTDOWN, DataQuerySnapshot.FIELD_DATA,
        DataQuerySnapshot.FIELD_INCOMPLETE_RESULTS_OK)));

    firstLevelFieldCombinations.add(new Fields(Sets.newHashSet(DataQuerySnapshot.FIELD_ID, DataQuerySnapshot.FIELD_TYPE,
        DataQuerySnapshot.FIELD_DATA, DataQuerySnapshot.FIELD_INCOMPLETE_RESULTS_OK)));

    firstLevelFieldCombinations.add(new Fields(Sets.newHashSet(DataQuerySnapshot.FIELD_ID, DataQuerySnapshot.FIELD_TYPE,
        DataQuerySnapshot.FIELD_COUNTDOWN, DataQuerySnapshot.FIELD_DATA)));

    firstLevelFieldCombinations.add(new Fields(Sets.newHashSet(DataQuerySnapshot.FIELD_ID, DataQuerySnapshot.FIELD_TYPE,
        DataQuerySnapshot.FIELD_DATA)));

    firstLevelFieldCombinations.add(new Fields(Sets.newHashSet(DataQuerySnapshot.FIELD_ID,
        DataQuerySnapshot.FIELD_TYPE)));

    FIRST_LEVEL_FIELD_COMBINATIONS = firstLevelFieldCombinations;

    Set<Fields> dataFieldCombinations = Sets.newHashSet();
    dataFieldCombinations.add(new Fields(Sets.newHashSet(DataQuerySnapshot.FIELD_SCHEMA_KEYS,
        DataQuerySnapshot.FIELD_FIELDS)));
    dataFieldCombinations.add(new Fields(Sets.newHashSet(DataQuerySnapshot.FIELD_SCHEMA_KEYS)));
    dataFieldCombinations.add(new Fields(Sets.newHashSet(DataQuerySnapshot.FIELD_FIELDS)));

    DATA_FIELD_COMBINATIONS = dataFieldCombinations;
  }

  /**
   * Constructor used to instantiate deserializer in {@link MessageDeserializerFactory}.
   */
  public DataQuerySnapshotDeserializer()
  {
  }

  @Override
  public Message deserialize(String json, Class<? extends Message> clazz, Object context) throws IOException
  {
    try {
      return deserializeHelper(json, context);
    } catch (Exception ex) {
      if (ex instanceof IOException) {
        throw (IOException)ex;
      } else {
        throw new IOException(ex);
      }
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
  private Message deserializeHelper(String json, Object context) throws Exception
  {
    JSONObject jo = new JSONObject(json);

    //Validate fields
    if (!SchemaUtils.checkValidKeys(jo, FIRST_LEVEL_FIELD_COMBINATIONS)) {
      throw new IOException("Invalid keys");
    }

    //// Query id stuff
    String id = jo.getString(DataQuerySnapshot.FIELD_ID);
    String type = jo.getString(DataQuerySnapshot.FIELD_TYPE);

    if (!type.equals(DataQuerySnapshot.TYPE)) {
      LOG.error("Found type {} in the query json, but expected type {}.", type, DataQuerySnapshot.TYPE);
      return null;
    }

    /// Countdown
    long countdown = -1L;
    boolean hasCountdown = jo.has(DataQuerySnapshot.FIELD_COUNTDOWN);

    if (hasCountdown) {
      countdown = jo.getLong(DataQuerySnapshot.FIELD_COUNTDOWN);
    }

    ////Data
    Map<String, String> schemaKeys = null;
    Set<String> fieldsSet = Sets.newHashSet();

    if (jo.has(DataQuerySnapshot.FIELD_DATA)) {
      JSONObject data = jo.getJSONObject(DataQuerySnapshot.FIELD_DATA);

      if (!SchemaUtils.checkValidKeys(data, DATA_FIELD_COMBINATIONS)) {
        LOG.error("Error validating {} field", DataQuerySnapshot.FIELD_DATA);
        throw new IOException("Invalid keys");
      }

      if (data.has(DataQuerySnapshot.FIELD_SCHEMA_KEYS)) {
        schemaKeys = SchemaUtils.extractMap(data.getJSONObject(DataQuerySnapshot.FIELD_SCHEMA_KEYS));
      }

      if (data.has(DataQuerySnapshot.FIELD_FIELDS)) {
        //// Fields
        JSONArray jArray = data.getJSONArray(DataQuerySnapshot.FIELD_FIELDS);

        for (int index = 0; index < jArray.length(); index++) {
          String field = jArray.getString(index);

          if (!fieldsSet.add(field)) {
            LOG.error("The field {} was listed more than once, this is an invalid query.", field);
          }
        }
      }
    }

    Fields fields = new Fields(fieldsSet);

    if (!hasCountdown) {
      return new DataQuerySnapshot(id, fields, schemaKeys);
    } else {
      return new DataQuerySnapshot(id, fields, countdown, schemaKeys);
    }
  }

  private static final Logger LOG = LoggerFactory.getLogger(DataQuerySnapshotDeserializer.class);
}
