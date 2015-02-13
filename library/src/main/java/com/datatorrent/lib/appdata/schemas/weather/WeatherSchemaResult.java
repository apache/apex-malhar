/*
 *  Copyright (c) 2012-2015 Malhar, Inc.
 *  All Rights Reserved.
 */

package com.datatorrent.lib.appdata.schemas.weather;

import com.datatorrent.lib.appdata.qr.QRType;
import com.datatorrent.lib.appdata.qr.Query;
import com.datatorrent.lib.appdata.qr.Result;
import com.datatorrent.lib.appdata.qr.ResultSerializerInfo;
import com.datatorrent.lib.appdata.qr.SimpleResultSerializer;
import com.datatorrent.lib.appdata.schemas.SchemaData;
import com.datatorrent.lib.appdata.schemas.SchemaValues;
import com.datatorrent.lib.appdata.schemas.TimeRangeIntervals;
import java.util.List;

/**
 *
 * @author Timothy Farkas: tim@datatorrent.com
 */
@QRType(type=WeatherSchemaResult.TYPE)
@ResultSerializerInfo(clazz=SimpleResultSerializer.class)
public class WeatherSchemaResult extends Result
{
  public static final String TYPE = "schemaData";

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
    private TimeRangeIntervals timeBuckets;
    private List<SchemaValues> values;

    /**
     * @return the timeBuckets
     */
    public TimeRangeIntervals getTimeBuckets()
    {
      return timeBuckets;
    }

    /**
     * @param timeBuckets the timeBuckets to set
     */
    public void setTimeBuckets(TimeRangeIntervals timeBuckets)
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
