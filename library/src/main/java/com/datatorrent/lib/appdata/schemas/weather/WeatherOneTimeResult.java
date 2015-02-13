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
import com.datatorrent.lib.appdata.schemas.TimeRangeIntervals;
import java.util.List;

/**
 *
 * @author Timothy Farkas: tim@datatorrent.com
 */

@QRType(type=WeatherOneTimeResult.TYPE)
@ResultSerializerInfo(clazz=SimpleResultSerializer.class)
public class WeatherOneTimeResult extends Result
{
  public static final String TYPE = "oneTimeResult";

  private WeatherOneTimeData data;

  public WeatherOneTimeResult(Query query)
  {
    super(query);
  }

  /**
   * @return the data
   */
  public WeatherOneTimeData getData()
  {
    return data;
  }

  /**
   * @param data the data to set
   */
  public void setData(WeatherOneTimeData data)
  {
    this.data = data;
  }

  public static class WeatherOneTimeData
  {
    private TimeRangeIntervals time;
    private List<WeatherDataData> data;

    /**
     * @return the time
     */
    public TimeRangeIntervals getTime()
    {
      return time;
    }

    /**
     * @param time the time to set
     */
    public void setTime(TimeRangeIntervals time)
    {
      this.time = time;
    }

    /**
     * @return the data
     */
    public List<WeatherDataData> getData()
    {
      return data;
    }

    /**
     * @param data the data to set
     */
    public void setData(List<WeatherDataData> data)
    {
      this.data = data;
    }
  }
}
