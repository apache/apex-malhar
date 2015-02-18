/*
 *  Copyright (c) 2012-2015 Malhar, Inc.
 *  All Rights Reserved.
 */

package com.datatorrent.lib.appdata.schemas;

import com.datatorrent.lib.appdata.qr.QRType;
import com.datatorrent.lib.appdata.qr.Query;
import com.datatorrent.lib.appdata.qr.QueryDeserializerInfo;
import com.datatorrent.lib.appdata.qr.SimpleQueryDeserializer;
import java.util.Map;

/**
 *
 * @author Timothy Farkas: tim@datatorrent.com
 */

@QRType(type=TimeSeriesTabularOneTimeQuery.TYPE)
@QueryDeserializerInfo(clazz=SimpleQueryDeserializer.class)
public class TabularUpdateQuery extends Query
{
  private Map<String, String> query;

  public TabularUpdateQuery()
  {
  }

  /**
   * @return the query
   */
  public Map<String, String> getQuery()
  {
    return query;
  }

  /**
   * @param query the query to set
   */
  public void setQuery(Map<String, String> query)
  {
    this.query = query;
  }
}
