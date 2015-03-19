/*
 *  Copyright (c) 2012-2015 Malhar, Inc.
 *  All Rights Reserved.
 */

package com.datatorrent.lib.appdata.qr.processor;

/**
 *
 * @author Timothy Farkas: tim@datatorrent.com
 */
public class QueryBundle<QUERY_TYPE, META_QUERY, QUEUE_CONTEXT>
{
  protected QUERY_TYPE query;
  protected META_QUERY metaQuery;
  protected QUEUE_CONTEXT queueContext;

  public QueryBundle()
  {
  }

  public QueryBundle(QUERY_TYPE query,
                     META_QUERY metaQuery,
                     QUEUE_CONTEXT queueContext)
  {
    this.query = query;
    this.metaQuery = metaQuery;
    this.queueContext = queueContext;
  }

  public QUERY_TYPE getQuery()
  {
    return query;
  }

  public META_QUERY getMetaQuery()
  {
    return metaQuery;
  }

  public QUEUE_CONTEXT getQueueContext()
  {
    return queueContext;
  }
}
