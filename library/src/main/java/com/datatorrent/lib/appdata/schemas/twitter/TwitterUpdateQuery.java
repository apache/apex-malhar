/*
 *  Copyright (c) 2012-2015 Malhar, Inc.
 *  All Rights Reserved.
 */

package com.datatorrent.lib.appdata.schemas.twitter;

import com.datatorrent.lib.appdata.Query;
import com.datatorrent.lib.appdata.QueryDeserializerInfo;
import com.datatorrent.lib.appdata.QuerySchemaInfo;
import com.datatorrent.lib.appdata.SimpleQueryDeserializer;

/**
 *
 * @author Timothy Farkas: tim@datatorrent.com
 */
@QuerySchemaInfo(type=TwitterUpdateQuery.TYPE)
@QueryDeserializerInfo(clazz=SimpleQueryDeserializer.class)
public class TwitterUpdateQuery extends Query
{
  public static final String TYPE = "updateQuery";
}
