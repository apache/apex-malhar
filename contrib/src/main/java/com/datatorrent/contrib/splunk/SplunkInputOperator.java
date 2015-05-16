/*
 *  Copyright (c) 2012-2015 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.datatorrent.contrib.splunk;

import javax.validation.constraints.NotNull;

/**
 * @since 2.1.0
 */
public class SplunkInputOperator extends AbstractSplunkInputOperator<String>
{
  @NotNull
  private String query = "search * | head 100";

  @Override
  public String getTuple(String value)
  {
    return value;
  }

  @Override
  public String queryToRetrieveData()
  {
    return query;
  }

  /*
   * Query to retrieve data from Splunk.
   */
  public String getQuery()
  {
    return query;
  }

  public void setQuery(String query)
  {
    this.query = query;
  }
}
