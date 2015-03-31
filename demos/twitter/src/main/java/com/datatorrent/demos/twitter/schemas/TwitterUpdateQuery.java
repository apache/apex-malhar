/*
 *  Copyright (c) 2012-2015 Malhar, Inc.
 *  All Rights Reserved.
 */

package com.datatorrent.demos.twitter.schemas;

import com.datatorrent.lib.appdata.qr.DataType;
import com.datatorrent.lib.appdata.qr.DataDeserializerInfo;
import com.datatorrent.lib.appdata.qr.SimpleDataDeserializer;


/**
 *
 * @author Timothy Farkas: tim@datatorrent.com
 */
@DataType(type=TwitterUpdateQuery.TYPE)
@DataDeserializerInfo(clazz=SimpleDataDeserializer.class)
public class TwitterUpdateQuery extends TwitterOneTimeQuery
{
  public static final String TYPE = "updateQuery";

  public TwitterUpdateQuery()
  {
  }
}
