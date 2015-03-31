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


/**
 *
 * @author Timothy Farkas: tim@datatorrent.com
 */
public class GenericDataResultTabularSerializer implements CustomDataSerializer
{
  public GenericDataResultTabularSerializer()
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
    GenericDataResultTabular gResult = (GenericDataResultTabular) result;

    JSONObject jo = new JSONObject();
    jo.put(Result.FIELD_ID, gResult.getId());
    jo.put(Result.FIELD_TYPE, gResult.getType());

    JSONArray ja = new JSONArray();

    for(GPOMutable value: gResult.getValues()) {
      JSONObject dataValue = GPOUtils.serializeJSONObject(value, gResult.getQuery().getFields());
      ja.put(dataValue);
    }

    jo.put(GenericDataResultTabular.FIELD_DATA, ja);

    return jo.toString();
  }
}
