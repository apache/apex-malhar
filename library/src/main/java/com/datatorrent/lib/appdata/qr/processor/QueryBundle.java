/*
 *  Copyright (c) 2012-2015 Malhar, Inc.
 *  All Rights Reserved.
 */

package com.datatorrent.lib.appdata.qr.processor;

import com.datatorrent.lib.appdata.qr.Query;

/**
 *
 * @author Timothy Farkas: tim@datatorrent.com
 */
public class QueryBundle<QUERY_TYPE extends Query, META_QUERY>
{
  protected QUERY_TYPE query;
  protected META_QUERY metaQuery;

  public QueryBundle()
  {
  }

  public QueryBundle(QUERY_TYPE query, META_QUERY metaQuery)
  {
    this.query = query;
    this.metaQuery = metaQuery;
  }

  public QUERY_TYPE getQuery()
  {
    return query;
  }

  public META_QUERY getMetaQuery()
  {
    return metaQuery;
  }
}
