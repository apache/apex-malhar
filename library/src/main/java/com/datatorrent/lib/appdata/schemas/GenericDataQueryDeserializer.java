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

import com.datatorrent.lib.appdata.dimensions.DimensionsDescriptor;
import com.datatorrent.lib.appdata.gpo.GPOImmutable;
import com.datatorrent.lib.appdata.gpo.GPOUtils;
import com.datatorrent.lib.appdata.qr.CustomDataDeserializer;
import com.datatorrent.lib.appdata.qr.Data;
import com.datatorrent.lib.appdata.qr.Query;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import org.codehaus.jettison.json.JSONArray;
import org.codehaus.jettison.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Iterator;
import java.util.List;
import java.util.Set;

public class GenericDataQueryDeserializer extends CustomDataDeserializer
{
  private static final Logger logger = LoggerFactory.getLogger(GenericDataQueryDeserializer.class);

  public GenericDataQueryDeserializer()
  {
  }

  @Override
  public Query deserialize(String json, Object context)
  {
    try {
      return deserializeHelper(json, context);
    }
    catch(Exception e) {
      logger.error("Exception while deserializing.", e);
      return null;
    }
  }

  private Query deserializeHelper(String json, Object context) throws Exception
  {
    GenericSchemaDimensional gsd = (GenericSchemaDimensional)context;
    JSONObject jo;

    jo = new JSONObject(json);

      //// Message
    String id = jo.getString(Query.FIELD_ID);
    String type = jo.getString(Data.FIELD_TYPE);

    boolean oneTime = !jo.has(GenericDataQueryTabular.FIELD_COUNTDOWN);
    long countdown = 1;

    if(!oneTime) {
      countdown = jo.getLong(GenericDataQuery.FIELD_COUNTDOWN);
    }

    boolean incompleteResultOK = jo.getBoolean(GenericDataQuery.FIELD_INCOMPLETE_RESULT_OK);

      //// Data
    JSONObject data = jo.getJSONObject(GenericDataQuery.FIELD_DATA);

    boolean hasFromTo = false;
    int latestNumBuckets = -1;
    String from = null;
    String to = null;
    TimeBucket bucket = null;

    boolean hasTime = data.has(GenericDataQuery.FIELD_TIME);

    if(hasTime) {
      //// Time
      JSONObject time = data.getJSONObject(GenericDataQuery.FIELD_TIME);

      if(time.has(GenericDataQuery.FIELD_FROM)
         ^ time.has(GenericDataQuery.FIELD_TO)) {
        logger.error("Both from and to must be specified, or netiher");
        return null;
      }

      hasFromTo = time.has(GenericDataQuery.FIELD_FROM);

      if(hasFromTo) {
        from = time.getString(GenericDataQuery.FIELD_FROM);
        to = time.getString(GenericDataQuery.FIELD_TO);
      }
      else {
        latestNumBuckets = time.getInt(GenericDataQuery.FIELD_LATEST_NUM_BUCKETS);
      }

      bucket = TimeBucket.BUCKET_TO_TYPE.get(time.getString(GenericDataQuery.FIELD_BUCKET));
    }

      //// Keys
    JSONObject keys = data.getJSONObject(GenericDataQuery.FIELD_KEYS);

    Iterator keyIterator = keys.keys();
    Set<String> keySet = Sets.newHashSet();

    while(keyIterator.hasNext()) {
      String key = (String)keyIterator.next();
      if(!keySet.add(key)) {
        logger.error("Duplicate key: {}", key);
        return null;
      }
    }

    List<String> fieldList = Lists.newArrayList();
    if(data.has(GenericDataQuery.FIELD_FIELDS)) {
      JSONArray fields = data.getJSONArray(GenericDataQuery.FIELD_FIELDS);

      for(int fieldIndex = 0;
          fieldIndex < fields.length();
          fieldIndex++) {
        String field = fields.getString(fieldIndex);
        fieldList.add(field);
      }
    }

    Fields queryFields = null;

    if(fieldList.isEmpty()) {
      if(hasTime) {
        fieldList.addAll(gsd.getValuesDescriptor().getFields().getFields());
        fieldList.add(DimensionsDescriptor.DIMENSION_TIME);

        queryFields = new Fields(fieldList);
      }
      else {
        queryFields = gsd.getValuesDescriptor().getFields();
      }
    }
    else {
      queryFields = new Fields(fieldList);
    }

    FieldsDescriptor keyFieldsDescriptor = gsd.getKeyFieldsDescriptor().getSubset(new Fields(keySet));
    GPOImmutable gpoIm = new GPOImmutable(GPOUtils.deserialize(keyFieldsDescriptor, keys));

    if(!hasTime) {
      return new GenericDataQuery(id,
                                  type,
                                  gpoIm,
                                  queryFields,
                                  incompleteResultOK);
    }
    else {
      if(oneTime) {
        if(hasFromTo) {
          return new GenericDataQuery(id,
                                      type,
                                      from,
                                      to,
                                      bucket,
                                      gpoIm,
                                      queryFields,
                                      incompleteResultOK);
        }
        else {
          return new GenericDataQuery(id,
                                      type,
                                      latestNumBuckets,
                                      bucket,
                                      gpoIm,
                                      queryFields,
                                      incompleteResultOK);
        }
      }
      else {
        if(hasFromTo) {
          return new GenericDataQuery(id,
                                      type,
                                      from,
                                      to,
                                      bucket,
                                      gpoIm,
                                      queryFields,
                                      countdown,
                                      incompleteResultOK);
        }
        else {
          return new GenericDataQuery(id,
                                      type,
                                      latestNumBuckets,
                                      bucket,
                                      gpoIm,
                                      queryFields,
                                      countdown,
                                      incompleteResultOK);
        }
      }
    }
  }
}
