/*
 *  Copyright (c) 2012-2015 Malhar, Inc.
 *  All Rights Reserved.
 */

package com.datatorrent.lib.appdata.schemas.weather;

import com.datatorrent.lib.appdata.qr.Query;
import com.datatorrent.lib.appdata.qr.QueryDeserializerInfo;
import com.datatorrent.lib.appdata.qr.QRType;
import com.datatorrent.lib.appdata.qr.SimpleQueryDeserializer;
import com.datatorrent.lib.appdata.schemas.SimpleTimeBucket;

/**
 *
 * @author Timothy Farkas: tim@datatorrent.com
 */
@QRType(type=WeatherUpdateQuery.TYPE)
@QueryDeserializerInfo(clazz=SimpleQueryDeserializer.class)
public class WeatherUpdateQuery extends Query
{
  public static final String TYPE = "updateQuery";

  private WeatherUpdateData data;

  public WeatherUpdateQuery()
  {
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
    private SimpleTimeBucket time;

    /**
     * @return the time
     */
    public SimpleTimeBucket getTime()
    {
      return time;
    }

    /**
     * @param time the time to set
     */
    public void setTime(SimpleTimeBucket time)
    {
      this.time = time;
    }
  }
}
