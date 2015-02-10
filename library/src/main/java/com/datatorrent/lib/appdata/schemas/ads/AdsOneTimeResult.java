/*
 *  Copyright (c) 2012-2015 Malhar, Inc.
 *  All Rights Reserved.
 */

package com.datatorrent.lib.appdata.schemas.ads;

import com.datatorrent.lib.appdata.Query;
import com.datatorrent.lib.appdata.Result;
import com.datatorrent.lib.appdata.ResultSerializerInfo;
import com.datatorrent.lib.appdata.SimpleResultSerializer;
import com.datatorrent.lib.appdata.schemas.TimeRangeBuckets;
import java.util.List;

/**
 *
 * @author Timothy Farkas: tim@datatorrent.com
 */
@ResultSerializerInfo(clazz=SimpleResultSerializer.class)
public class AdsOneTimeResult extends Result
{
  private AdsOneTimeData data;

  public AdsOneTimeResult(Query query)
  {
    super(query);
  }

  /**
   * @return the data
   */
  public AdsOneTimeData getData()
  {
    return data;
  }

  /**
   * @param data the data to set
   */
  public void setData(AdsOneTimeData data)
  {
    this.data = data;
  }

  public static class AdsOneTimeData
  {
    private TimeRangeBuckets time;
    private AdsKeys keys;
    private List<AdsDataData> data;

    /**
     * @return the time
     */
    public TimeRangeBuckets getTime()
    {
      return time;
    }

    /**
     * @param time the time to set
     */
    public void setTime(TimeRangeBuckets time)
    {
      this.time = time;
    }

    /**
     * @return the keys
     */
    public AdsKeys getKeys()
    {
      return keys;
    }

    /**
     * @param keys the keys to set
     */
    public void setKeys(AdsKeys keys)
    {
      this.keys = keys;
    }

    /**
     * @return the data
     */
    public List<AdsDataData> getData()
    {
      return data;
    }

    /**
     * @param data the data to set
     */
    public void setData(List<AdsDataData> data)
    {
      this.data = data;
    }
  }
}
