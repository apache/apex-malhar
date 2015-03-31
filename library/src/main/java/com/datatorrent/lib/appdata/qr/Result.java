/*
 *  Copyright (c) 2012-2015 Malhar, Inc.
 *  All Rights Reserved.
 */

package com.datatorrent.lib.appdata.qr;

import com.google.common.base.Preconditions;

/**
 *
 * @author Timothy Farkas: tim@datatorrent.com
 */
public class Result
{
  public static final String FIELD_ID = "id";
  public static final String FIELD_TYPE = "type";
  public static final String FIELD_DATA = "data";

  private String id;
  private String type;
  private Query query;

  public Result()
  {
  }

  public Result(Query query)
  {
    setQuery(query);
  }

  private void setQuery(Query query)
  {
    Preconditions.checkNotNull(query);
    this.query = query;
  }

  public Query getQuery()
  {
    return query;
  }

  public String getId()
  {
    return query.getId();
  }

  /**
   * @return the type
   */
  public String getType()
  {
    return type;
  }

  /**
   * @param type the type to set
   */
  public void setType(String type)
  {
    this.type = type;
  }
}
