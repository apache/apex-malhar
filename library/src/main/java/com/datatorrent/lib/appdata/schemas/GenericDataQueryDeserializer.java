/*
 *  Copyright (c) 2012-2015 Malhar, Inc.
 *  All Rights Reserved.
 */

package com.datatorrent.lib.appdata.schemas;

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

    boolean oneTime = !jo.has(GenericDataQuery.FIELD_COUNTDOWN);
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

    FieldsDescriptor keyFieldsDescriptor = gsd.getKeyFieldsDescriptor().getSubset(new Fields(keySet));
    GPOImmutable gpoIm = new GPOImmutable(GPOUtils.deserialize(keyFieldsDescriptor, keys));

    if(!hasTime) {
      return new GenericDataQuery(gpoIm,
                                  new Fields(fieldList),
                                  incompleteResultOK);
    }
    else {
      if(oneTime) {
        if(hasFromTo) {
          return new GenericDataQuery(from,
                                      to,
                                      bucket,
                                      gpoIm,
                                      new Fields(fieldList),
                                      incompleteResultOK);
        }
        else {
          return new GenericDataQuery(latestNumBuckets,
                                      bucket,
                                      gpoIm,
                                      new Fields(fieldList),
                                      incompleteResultOK);
        }
      }
      else {
        if(hasFromTo) {
          return new GenericDataQuery(from,
                                      to,
                                      bucket,
                                      gpoIm,
                                      new Fields(fieldList),
                                      countdown,
                                      incompleteResultOK);
        }
        else {
          return new GenericDataQuery(latestNumBuckets,
                                      bucket,
                                      gpoIm,
                                      new Fields(fieldList),
                                      countdown,
                                      incompleteResultOK);
        }
      }
    }
  }
}
