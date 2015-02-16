/*
 *  Copyright (c) 2012-2015 Malhar, Inc.
 *  All Rights Reserved.
 */

package com.datatorrent.lib.appdata.qr.processor;

import com.datatorrent.api.Context.OperatorContext;
import com.datatorrent.lib.appdata.qr.Query;
import com.datatorrent.lib.appdata.qr.Result;

/**
 *
 * @author Timothy Farkas: tim@datatorrent.com
 */
public class NOPQueryResultCacheManager<QUERY_TYPE extends Query, META_QUERY> implements QueryResultCacheManager<QUERY_TYPE, META_QUERY>
{
  @Override
  public Result getResult(QUERY_TYPE query, META_QUERY metaQuery)
  {
    return null;
  }

  @Override
  public void setup(OperatorContext context)
  {
  }

  @Override
  public void beginWindow(long windowId)
  {
  }

  @Override
  public void endWindow()
  {
  }

  @Override
  public void teardown()
  {
  }
}
