/*
 *  Copyright (c) 2012-2015 Malhar, Inc.
 *  All Rights Reserved.
 */

package com.datatorrent.lib.appdata.qr.processor;

import org.apache.commons.lang3.mutable.MutableBoolean;

/**
 *
 * @author Timothy Farkas: tim@datatorrent.com
 */
public class SimpleDoneQueryQueueManager<QUERY_TYPE, META_QUERY> extends
AbstractWEQueryQueueManager<QUERY_TYPE, META_QUERY, MutableBoolean>
{
  private QueueList<QueryBundle<QUERY_TYPE, META_QUERY, MutableBoolean>> queryQueue;

  public SimpleDoneQueryQueueManager()
  {
  }

  @Override
  public boolean removeBundle(QueryBundle<QUERY_TYPE, META_QUERY, MutableBoolean> queryQueueable)
  {
    return queryQueueable.getQueueContext().booleanValue();
  }
}
