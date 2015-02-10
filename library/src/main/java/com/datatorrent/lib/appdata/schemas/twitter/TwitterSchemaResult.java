/*
 *  Copyright (c) 2012-2015 Malhar, Inc.
 *  All Rights Reserved.
 */

package com.datatorrent.lib.appdata.schemas.twitter;

import com.datatorrent.lib.appdata.Query;
import com.datatorrent.lib.appdata.Result;
import com.datatorrent.lib.appdata.ResultSerializerInfo;
import com.datatorrent.lib.appdata.SimpleResultSerializer;
import com.datatorrent.lib.appdata.schemas.SchemaData;
import java.util.List;

/**
 *
 * @author Timothy Farkas: tim@datatorrent.com
 */
@ResultSerializerInfo(clazz=SimpleResultSerializer.class)
public class TwitterSchemaResult extends Result
{
  private TwitterSchemaData data;

  public TwitterSchemaResult(Query query)
  {
    super(query);
  }

  /**
   * @return the data
   */
  public TwitterSchemaData getData()
  {
    return data;
  }

  /**
   * @param data the data to set
   */
  public void setData(TwitterSchemaData data)
  {
    this.data = data;
  }

  public static class TwitterSchemaData extends SchemaData
  {
    private List<TwitterDataValues> values;

    /**
     * @return the values
     */
    public List<TwitterDataValues> getValues()
    {
      return values;
    }

    /**
     * @param values the values to set
     */
    public void setValues(List<TwitterDataValues> values)
    {
      this.values = values;
    }
  }
}
