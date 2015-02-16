/*
 *  Copyright (c) 2012-2015 Malhar, Inc.
 *  All Rights Reserved.
 */

package com.datatorrent.lib.appdata.qr.processor;

import com.datatorrent.lib.appdata.qr.Query;
import com.google.common.base.Preconditions;

/**
 *
 * @author Timothy Farkas: tim@datatorrent.com
 */
public class QueryQueueable<QUERY_TYPE extends Query, META_QUERY, QUEUE_CONTEXT>
extends QueryBundle<QUERY_TYPE, META_QUERY>
{
  private QUEUE_CONTEXT queueContext;

  public QueryQueueable(QUERY_TYPE query, META_QUERY metaQuery, QUEUE_CONTEXT queueContext)
  {
    setQuery(query);
    setMetaQuery(metaQuery);
    setQueueContext(queueContext);
  }

  private void setQuery(QUERY_TYPE query)
  {
    Preconditions.checkNotNull(query);
    this.query = query;
  }

  private void setQueueContext(QUEUE_CONTEXT queueContext)
  {
    this.queueContext = queueContext;
  }

  public QUEUE_CONTEXT getQueueContext()
  {
    return queueContext;
  }

  /**
   * @param metaQuery the metaQuery to set
   */
  private void setMetaQuery(META_QUERY metaQuery)
  {
    this.metaQuery = metaQuery;
  }

  @Override
  public String toString()
  {
    return "QueryQueueable{" + "query=" + query +
           ", queueContext=" + queueContext +
           ", metaQuery=" + metaQuery + '}';
  }
}
