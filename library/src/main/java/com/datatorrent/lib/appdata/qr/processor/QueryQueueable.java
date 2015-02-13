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
public class QueryQueueable<QUEUE_CONTEXT>
{
  private Query query;
  private QUEUE_CONTEXT queueContext;

  public QueryQueueable(Query query, QUEUE_CONTEXT queueContext)
  {
    setQuery(query);
    setQueueContext(queueContext);
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

  private void setQueueContext(QUEUE_CONTEXT queueContext)
  {
    Preconditions.checkNotNull(queueContext);
    this.queueContext = queueContext;
  }

  public QUEUE_CONTEXT getQueueContext()
  {
    return queueContext;
  }
}
