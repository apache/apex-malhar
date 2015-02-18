/*
 *  Copyright (c) 2012-2015 Malhar, Inc.
 *  All Rights Reserved.
 */

package com.datatorrent.lib.appdata.schemas;

import com.datatorrent.lib.appdata.qr.QRType;
import com.datatorrent.lib.appdata.qr.QueryDeserializerInfo;
import com.datatorrent.lib.appdata.qr.SimpleQueryDeserializer;

/**
 *
 * @author Timothy Farkas: tim@datatorrent.com
 */

@QRType(type=TimeSeriesTabularUpdateQuery.TYPE)
@QueryDeserializerInfo(clazz=SimpleQueryDeserializer.class)
public class TimeSeriesTabularUpdateQuery extends TabularUpdateQuery
{
  public static final String TYPE = "updateQuery";

  public TimeSeriesTabularUpdateQuery()
  {
  }
}
