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
public class TabularSchemaResult extends Result
{
  private SchemaData data;

  public TabularSchemaResult(Query query)
  {
    super(query);
  }

  public static class TabularSchemaData extends SchemaData
  {
    private List<Map<String, String>> values;

    public TabularSchemaData()
    {
    }

    /**
     * @return the values
     */
    public List<Map<String, String>> getValues()
    {
      return values;
    }

    /**
     * @param values the values to set
     */
    public void setValues(List<Map<String, String>> values)
    {
      this.values = values;
    }
  }
}
