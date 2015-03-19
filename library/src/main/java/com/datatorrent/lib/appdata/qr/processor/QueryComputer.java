/*
 *  Copyright (c) 2012-2015 Malhar, Inc.
 *  All Rights Reserved.
 */

package com.datatorrent.lib.appdata.qr.processor;

/**
 *
 * @author Timothy Farkas: tim@datatorrent.com
 */
public interface QueryComputer<QUERY_TYPE, META_QUERY, QUEUE_CONTEXT, COMPUTE_CONTEXT, RESULT>
{
  public RESULT processQuery(QUERY_TYPE query,
                             META_QUERY metaQuery,
                             QUEUE_CONTEXT queueContext,
                             COMPUTE_CONTEXT context);
  public void queueDepleted(COMPUTE_CONTEXT context);
}
