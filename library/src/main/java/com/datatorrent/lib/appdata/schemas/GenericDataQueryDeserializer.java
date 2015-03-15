/*
 *  Copyright (c) 2012-2015 Malhar, Inc.
 *  All Rights Reserved.
 */

package com.datatorrent.lib.appdata.schemas;

import com.datatorrent.lib.appdata.qr.CustomDataDeserializer;
import com.datatorrent.lib.appdata.qr.Data;
import com.datatorrent.lib.appdata.qr.Query;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 *
 * @author Timothy Farkas: tim@datatorrent.com
 */
public class GenericDataQueryDeserializer extends CustomDataDeserializer
{
  private static final Logger logger = LoggerFactory.getLogger(GenericDataQueryDeserializer.class);

  public GenericDataQueryDeserializer()
  {
  }

  @Override
  public Query deserialize(String json)
  {
    JSONObject jo;

    try {
      jo = new JSONObject(json);

      //// Message

      String id = jo.getString(Query.FIELD_ID);
      String type = jo.getString(Data.FIELD_TYPE);

      int countdown = jo.getInt(GenericDataQuery.FIELD_COUNTDOWN);
      boolean incompleteResultOK = jo.getBoolean(GenericDataQuery.FIELD_INCOMPLETE_RESULT_OK);

      //// Data

      JSONObject data = jo.getJSONObject(GenericDataQuery.FIELD_DATA);

      //// Time

      JSONObject time = data.getJSONObject(GenericDataQuery.FIELD_TIME);

      String from = time.getString(GenericDataQuery.FIELD_FROM);
      String to = time.getString(GenericDataQuery.FIELD_TO);

      int latestNumBuckets = time.getInt(GenericDataQuery.FIELD_LATEST_NUM_BUCKETS);
      TimeBucket bucket = TimeBucket.BUCKET_TO_TYPE.get(time.getString(GenericDataQuery.FIELD_BUCKET));

      //// Keys

      JSONObject keys = data.getJSONObject(GenericDataQuery.FIELD_KEYS);
      //GPOMutable mutableGPO = GPOUtils.deserialize(keys);


    }
    catch(JSONException ex) {
      return null;
    }

    return null;
  }

  private Query deserializeHelper(String json) throws Exception
  {
    return null;
  }
}
