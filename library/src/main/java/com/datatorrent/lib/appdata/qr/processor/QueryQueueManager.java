/*
 *  Copyright (c) 2012-2015 Malhar, Inc.
 *  All Rights Reserved.
 */

package com.datatorrent.lib.appdata.qr.processor;

import com.datatorrent.api.Operator;
import com.datatorrent.lib.appdata.qr.Query;

/**
 *
 * @author Timothy Farkas: tim@datatorrent.com
 */
public interface QueryQueueManager<QUEUE_CONTEXT> extends Operator
{
  public boolean enqueue(Query query, QUEUE_CONTEXT queueContext);
  public Query dequeue();
}
