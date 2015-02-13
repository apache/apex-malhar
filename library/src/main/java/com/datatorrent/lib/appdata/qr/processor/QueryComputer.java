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
public interface QueryComputer
{
  public Result processQuery(Query query);
}
