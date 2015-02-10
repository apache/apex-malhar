/*
 *  Copyright (c) 2012-2015 Malhar, Inc.
 *  All Rights Reserved.
 */

package com.datatorrent.lib.appdata.schemas;

import com.datatorrent.lib.appdata.QueryDeserializerInfo;
import com.datatorrent.lib.appdata.QuerySchemaInfo;
import com.datatorrent.lib.appdata.SimpleQueryDeserializer;
import com.datatorrent.lib.appdata.schemas.UpdateQuery;

/**
 *
 * @author Timothy Farkas: tim@datatorrent.com
 */
@QuerySchemaInfo(type=OneTimeQuery.ONE_TIME_QUERY_TYPE)
@QueryDeserializerInfo(clazz=SimpleQueryDeserializer.class)
public class OneTimeQuery extends UpdateQuery
{
  public static final String ONE_TIME_QUERY_TYPE = "oneTimeQuery";

  @Override
  public String getType()
  {
    return ONE_TIME_QUERY_TYPE;
  }
}
