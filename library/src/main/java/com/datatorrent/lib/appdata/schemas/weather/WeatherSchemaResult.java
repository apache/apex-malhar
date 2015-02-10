/*
 *  Copyright (c) 2012-2015 Malhar, Inc.
 *  All Rights Reserved.
 */

package com.datatorrent.lib.appdata.schemas.weather;

import com.datatorrent.lib.appdata.Query;
import com.datatorrent.lib.appdata.Result;
import com.datatorrent.lib.appdata.ResultSerializerInfo;
import com.datatorrent.lib.appdata.SimpleResultSerializer;
import com.datatorrent.lib.appdata.schemas.SchemaData;
import com.datatorrent.lib.appdata.schemas.SchemaValues;
import com.datatorrent.lib.appdata.schemas.TimeRangeBuckets;
import java.util.List;

/**
 *
 * @author Timothy Farkas: tim@datatorrent.com
 */
@ResultSerializerInfo(clazz=SimpleResultSerializer.class)
public class WeatherSchemaResult extends Result
{
  private WeatherSchemaData data;

  public WeatherSchemaResult(Query query)
  {
    super(query);
  }

  /**
   * @return the data
   */
  public WeatherSchemaData getData()
  {
    return data;
  }

  /**
   * @param data the data to set
   */
  public void setData(WeatherSchemaData data)
  {
    this.data = data;
  }

  public static class WeatherSchemaData extends SchemaData
  {
    private TimeRangeBuckets timeBuckets;
    private List<SchemaValues> values;

    /**
     * @return the timeBuckets
     */
    public TimeRangeBuckets getTimeBuckets()
    {
      return timeBuckets;
    }

    /**
     * @param timeBuckets the timeBuckets to set
     */
    public void setTimeBuckets(TimeRangeBuckets timeBuckets)
    {
      this.timeBuckets = timeBuckets;
    }

    /**
     * @return the values
     */
    public List<SchemaValues> getValues()
    {
      return values;
    }

    /**
     * @param values the values to set
     */
    public void setValues(List<SchemaValues> values)
    {
      this.values = values;
    }
  }
}
