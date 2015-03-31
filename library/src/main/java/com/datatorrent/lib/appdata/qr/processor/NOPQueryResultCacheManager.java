/*
 *  Copyright (c) 2012-2015 Malhar, Inc.
 *  All Rights Reserved.
 */

package com.datatorrent.lib.appdata.qr.processor;

import com.datatorrent.api.Context.OperatorContext;

/**
 *
 * @author Timothy Farkas: tim@datatorrent.com
 */
public class NOPQueryResultCacheManager<QUERY_TYPE, META_QUERY, RESULT> implements QueryResultCacheManager<QUERY_TYPE, META_QUERY, RESULT>
{
  @Override
  public RESULT getResult(QUERY_TYPE query, META_QUERY metaQuery)
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
