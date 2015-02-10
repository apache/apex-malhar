/*
 *  Copyright (c) 2012-2015 Malhar, Inc.
 *  All Rights Reserved.
 */

package com.datatorrent.lib.appdata.schemas.weather;

import com.datatorrent.lib.appdata.Query;
import com.datatorrent.lib.appdata.QueryDeserializerInfo;
import com.datatorrent.lib.appdata.QuerySchemaInfo;
import com.datatorrent.lib.appdata.SimpleQueryDeserializer;
import com.datatorrent.lib.appdata.schemas.SimpleTimeBucket;

/**
 *
 * @author Timothy Farkas: tim@datatorrent.com
 */
@QuerySchemaInfo(type=WeatherUpdateQuery.TYPE)
@QueryDeserializerInfo(clazz=SimpleQueryDeserializer.class)
public class WeatherUpdateQuery extends Query
{
  public static final String TYPE = "updateQuery";

  private WeatherUpdateData data;

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
