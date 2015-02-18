/*
 *  Copyright (c) 2012-2015 Malhar, Inc.
 *  All Rights Reserved.
 */

package com.datatorrent.lib.appdata.schemas;

import com.datatorrent.lib.appdata.qr.Query;

/**
 *
 * @author Timothy Farkas: tim@datatorrent.com
 */
public class DimensionalOneTimeQuery extends TimeSeriesTabularOneTimeResult
{

  public DimensionalOneTimeQuery(Query query)
  {
    super(query);
  }

}
