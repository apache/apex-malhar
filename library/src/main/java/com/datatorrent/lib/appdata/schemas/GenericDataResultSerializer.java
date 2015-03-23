/*
 *  Copyright (c) 2012-2015 Malhar, Inc.
 *  All Rights Reserved.
 */

package com.datatorrent.lib.appdata.schemas;

import com.datatorrent.lib.appdata.gpo.GPOMutable;
import com.datatorrent.lib.appdata.gpo.GPOUtils;
import com.datatorrent.lib.appdata.qr.CustomDataSerializer;
import com.datatorrent.lib.appdata.qr.Result;
import org.codehaus.jettison.json.JSONArray;
import org.codehaus.jettison.json.JSONObject;

import java.util.List;

/**
 *
 * @author Timothy Farkas: tim@datatorrent.com
 */
public class GenericDataResultSerializer implements CustomDataSerializer
{
  public GenericDataResultSerializer()
  {
  }

  @Override
  public String serialize(Result result)
  {
    try {
      return serializeHelper(result);
    }
    catch(Exception e) {
      throw new RuntimeException(e);
    }
  }

  private String serializeHelper(Result result) throws Exception
  {
    GenericDataResult dataResult = (GenericDataResult) result;

    JSONObject jo = new JSONObject();

    jo.put(Result.FIELD_ID, dataResult.getId());
    jo.put(Result.FIELD_TYPE, dataResult.getType());

    JSONArray data = new JSONArray();
    jo.put(Result.FIELD_DATA, data);

    Fields fields = dataResult.getDataQuery().getFields();
    List<GPOMutable> values = dataResult.getValues();

    for(GPOMutable value: values) {
      JSONObject valueJO = GPOUtils.serializeJSONObject(value, fields);
      data.put(valueJO);
    }

    if(!dataResult.getDataQuery().getOneTime()) {
      jo.put(GenericDataResult.FIELD_COUNTDOWN,
             dataResult.getCountdown());
    }

    return jo.toString();
  }
}
