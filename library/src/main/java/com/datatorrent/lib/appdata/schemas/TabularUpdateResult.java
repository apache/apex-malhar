/*
 *  Copyright (c) 2012-2015 Malhar, Inc.
 *  All Rights Reserved.
 */

package com.datatorrent.lib.appdata.schemas;

import com.datatorrent.lib.appdata.qr.Query;
import com.datatorrent.lib.appdata.qr.Result;
import java.util.List;
import java.util.Map;

/**
 *
 * @author Timothy Farkas: tim@datatorrent.com
 */
public class TabularUpdateResult extends Result
{
  private TabularUpdateResultData data;


  public TabularUpdateResult(Query query)
  {
    super(query);
  }

  /**
   * @return the data
   */
  public TabularUpdateResultData getData()
  {
    return data;
  }

  /**
   * @param data the data to set
   */
  public void setData(TabularUpdateResultData data)
  {
    this.data = data;
  }

  public static class TabularUpdateResultData
  {
    private List<Map<String, Object>> values;

    public TabularUpdateResultData()
    {
    }

    /**
     * @return the values
     */
    public List<Map<String, Object>> getValues()
    {
      return values;
    }

    /**
     * @param values the values to set
     */
    public void setValues(List<Map<String, Object>> values)
    {
      this.values = values;
    }
  }
}
