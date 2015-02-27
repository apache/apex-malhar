/*
 *  Copyright (c) 2012-2015 Malhar, Inc.
 *  All Rights Reserved.
 */

package com.datatorrent.demos.twitter.schemas;

import com.datatorrent.lib.appdata.qr.QRType;
import com.datatorrent.lib.appdata.qr.QueryDeserializerInfo;
import com.datatorrent.lib.appdata.qr.SimpleQueryDeserializer;


/**
 *
 * @author Timothy Farkas: tim@datatorrent.com
 */
@QRType(type=TwitterUpdateQuery.TYPE)
@QueryDeserializerInfo(clazz=SimpleQueryDeserializer.class)
public class TwitterUpdateQuery extends TwitterOneTimeQuery
{
  public static final String TYPE = "updateQuery";

  public TwitterUpdateQuery()
  {
  }
}
