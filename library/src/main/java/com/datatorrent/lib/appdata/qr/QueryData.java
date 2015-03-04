/*
 *  Copyright (c) 2012-2015 Malhar, Inc.
 *  All Rights Reserved.
 */

package com.datatorrent.lib.appdata.qr;

import java.util.List;

/**
 *
 * @author Timothy Farkas: tim@datatorrent.com
 */
public class QueryData
{
  private List<String> fields;

  public QueryData()
  {
  }

  public void setFields(List<String> fields)
  {
    this.fields = fields;
  }

  public List<String> getFields()
  {
    return fields;
  }
}
