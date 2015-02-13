/*
 *  Copyright (c) 2012-2015 Malhar, Inc.
 *  All Rights Reserved.
 */

package com.datatorrent.lib.appdata.schemas.ads;

import com.datatorrent.lib.appdata.qr.Query;
import com.datatorrent.lib.appdata.qr.QueryDeserializerInfo;
import com.datatorrent.lib.appdata.qr.QRType;
import com.datatorrent.lib.appdata.qr.SimpleQueryDeserializer;
import com.datatorrent.lib.appdata.schemas.TimeRangeBucket;

@QRType(type=AdsOneTimeQuery.TYPE)
@QueryDeserializerInfo(clazz=SimpleQueryDeserializer.class)
public class AdsOneTimeQuery extends Query
{
  public static final String TYPE = "oneTimeQuery";

  private AdsOneTimeQueryData data;

  public AdsOneTimeQuery()
  {
  }

  /**
   * @return the data
   */
  public AdsOneTimeQueryData getData()
  {
    return data;
  }

  /**
   * @param data the data to set
   */
  public void setData(AdsOneTimeQueryData data)
  {
    this.data = data;
  }

  public static class AdsOneTimeQueryData
  {
    private TimeRangeBucket time;
    private AdsKeys keys;

    public AdsOneTimeQueryData()
    {
    }

    /**
     * @return the timeRangeBucket
     */
    public TimeRangeBucket getTime()
    {
      return time;
    }

    /**
     * @param time the timeRangeBucket to set
     */
    public void setTime(TimeRangeBucket time)
    {
      this.time = time;
    }

    /**
     * @return the adsKeys
     */
    public AdsKeys getKeys()
    {
      return keys;
    }

    /**
     * @param keys the adsKeys to set
     */
    public void setKeys(AdsKeys keys)
    {
      this.keys = keys;
    }
  }
}
