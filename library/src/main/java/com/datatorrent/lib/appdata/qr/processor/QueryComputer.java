/*
 *  Copyright (c) 2012-2015 Malhar, Inc.
 *  All Rights Reserved.
 */

package com.datatorrent.lib.appdata.qr.processor;

import com.datatorrent.lib.appdata.qr.Query;
import com.datatorrent.lib.appdata.qr.Result;

/**
 *
 * @author Timothy Farkas: tim@datatorrent.com
 */
public interface QueryComputer<QUERY_TYPE extends Query, META_QUERY, COMPUTE_CONTEXT>
{
  public Result processQuery(QUERY_TYPE query, META_QUERY metaQuery, COMPUTE_CONTEXT context);
  public void queueDepleted(COMPUTE_CONTEXT context);
}
