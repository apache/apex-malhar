/*
 *  Copyright (c) 2012-2015 Malhar, Inc.
 *  All Rights Reserved.
 */

package com.datatorrent.lib.appdata.schemas.ads;

import com.datatorrent.lib.appdata.qr.QRType;
import com.datatorrent.lib.appdata.qr.Query;
import com.datatorrent.lib.appdata.qr.Result;
import com.datatorrent.lib.appdata.qr.ResultSerializerInfo;
import com.datatorrent.lib.appdata.qr.SimpleResultSerializer;
import com.datatorrent.lib.appdata.schemas.TimeRangeBucket;
import java.util.List;

/**
 *
 * @author Timothy Farkas: tim@datatorrent.com
 */
@QRType(type=AdsOneTimeResult.TYPE)
@ResultSerializerInfo(clazz=SimpleResultSerializer.class)
public class AdsOneTimeResult extends Result
{
  public static final String TYPE = "oneTimeResult";
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
    private TimeRangeBucket time;
    private AdsKeys keys;
    private List<AdsDataData> data;

    /**
     * @return the time
     */
    public TimeRangeBucket getTime()
    {
      return time;
    }

    /**
     * @param time the time to set
     */
    public void setTime(TimeRangeBucket time)
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
