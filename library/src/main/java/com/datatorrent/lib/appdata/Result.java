/*
 *  Copyright (c) 2012-2015 Malhar, Inc.
 *  All Rights Reserved.
 */

package com.datatorrent.lib.appdata;

/**
 *
 * @author Timothy Farkas: tim@datatorrent.com
 */
public abstract class Result
{
  private String id;

  public Result(Query query)
  {
    this.id = query.getId();
  }

  public String getId()
  {
    return id;
  }

  public abstract String getType();
}
