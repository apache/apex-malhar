/*
 *  Copyright (c) 2012-2015 Malhar, Inc.
 *  All Rights Reserved.
 */

package com.datatorrent.lib.appdata.schemas;

import com.datatorrent.lib.appdata.qr.CustomResultSerializer;
import com.datatorrent.lib.appdata.qr.Result;


/**
 *
 * @author Timothy Farkas: tim@datatorrent.com
 */
public class GenericSchemaSerializer implements CustomResultSerializer
{
  public GenericSchemaSerializer()
  {
    //Do nothing
  }

  @Override
  public String serialize(Result result)
  {
    if(!(result instanceof GenericSchemaResult))
    {
      throw new IllegalArgumentException("Must receive a "
                                         + GenericSchemaResult.class
                                         + " object.");
    }

    GenericSchemaResult genericSchemaResult = (GenericSchemaResult) result;

    StringBuilder sb = new StringBuilder();

    sb.append("{\"").append(Result.FIELD_ID).
    append("\":\"").append(result.getId()).
    append("\",\"").append(Result.FIELD_TYPE).
    append("\":\"").append(result.getType()).
    append("\",\"").append(Result.FIELD_DATA).
    append("\":").append(genericSchemaResult.getGenericSchema().getSchemaJSON()).
    append("}");

    return sb.toString();
  }
}
