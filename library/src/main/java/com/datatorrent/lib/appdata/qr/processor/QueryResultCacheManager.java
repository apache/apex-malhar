/*
 *  Copyright (c) 2012-2015 Malhar, Inc.
 *  All Rights Reserved.
 */

package com.datatorrent.lib.appdata.qr.processor;

import com.datatorrent.api.Operator;

/**
 *
 * @author Timothy Farkas: tim@datatorrent.com
 */
public interface QueryResultCacheManager<QUERY_TYPE, META_QUERY, RESULT> extends Operator
{
  public RESULT getResult(QUERY_TYPE query, META_QUERY metaQuery);
}
