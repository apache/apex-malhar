/*
 *  Copyright (c) 2012-2015 Malhar, Inc.
 *  All Rights Reserved.
 */

package com.datatorrent.lib.appdata.schemas.weather;

import com.datatorrent.lib.appdata.Query;
import com.datatorrent.lib.appdata.Result;
import com.datatorrent.lib.appdata.ResultSerializerInfo;
import com.datatorrent.lib.appdata.SimpleResultSerializer;

/**
 *
 * @author Timothy Farkas: tim@datatorrent.com
 */
@ResultSerializerInfo(clazz=SimpleResultSerializer.class)
public class WeatherUpdateResult extends Result
{
  private WeatherUpdateData data;

  public WeatherUpdateResult(Query query)
  {
    super(query);
  }

  /**
   * @return the data
   */
  public WeatherUpdateData getData()
  {
    return data;
  }

  /**
   * @param data the data to set
   */
  public void setData(WeatherUpdateData data)
  {
    this.data = data;
  }

  public static class WeatherUpdateData
  {
    private String time;
    private String bucket;
    private WeatherValues values;

    /**
     * @return the time
     */
    public String getTime()
    {
      return time;
    }

    /**
     * @param time the time to set
     */
    public void setTime(String time)
    {
      this.time = time;
    }

    /**
     * @return the bucket
     */
    public String getBucket()
    {
      return bucket;
    }

    /**
     * @param bucket the bucket to set
     */
    public void setBucket(String bucket)
    {
      this.bucket = bucket;
    }

    /**
     * @return the values
     */
    public WeatherValues getValues()
    {
      return values;
    }

    /**
     * @param values the values to set
     */
    public void setValues(WeatherValues values)
    {
      this.values = values;
    }
  }
}
