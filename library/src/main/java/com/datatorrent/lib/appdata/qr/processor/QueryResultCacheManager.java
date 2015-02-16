/*
 *  Copyright (c) 2012-2015 Malhar, Inc.
 *  All Rights Reserved.
 */

package com.datatorrent.lib.appdata.qr.processor;

import com.datatorrent.api.Operator;
import com.datatorrent.lib.appdata.qr.Query;
import com.datatorrent.lib.appdata.qr.Result;

/**
 *
 * @author Timothy Farkas: tim@datatorrent.com
 */
public interface QueryResultCacheManager<QUERY_TYPE extends Query, META_QUERY> extends Operator
{
  public Result getResult(QUERY_TYPE query, META_QUERY metaQuery);
}
