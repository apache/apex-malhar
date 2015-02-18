/*
 *  Copyright (c) 2012-2015 Malhar, Inc.
 *  All Rights Reserved.
 */

package com.datatorrent.lib.appdata.schemas.ads;

import com.datatorrent.lib.appdata.qr.Query;
import com.datatorrent.lib.appdata.qr.QueryDeserializerInfo;
import com.datatorrent.lib.appdata.qr.QRType;
import com.datatorrent.lib.appdata.qr.QueryValidatorInfo;
import com.datatorrent.lib.appdata.qr.SimpleQueryDeserializer;
import com.datatorrent.lib.appdata.qr.SimpleQueryValidator;
import com.datatorrent.lib.appdata.schemas.SimpleTimeBucket;

/**
 *
 * @author Timothy Farkas: tim@datatorrent.com
 */
@QRType(type=AdsUpdateQuery.TYPE)
@QueryDeserializerInfo(clazz=SimpleQueryDeserializer.class)
@QueryValidatorInfo(clazz=SimpleQueryValidator.class)
public class AdsUpdateQuery extends Query
{
  public static final String TYPE = "updateQuery";

  private AdsUpdateQueryData data;

  public AdsUpdateQuery()
  {
  }

  public AdsUpdateQueryData getData()
  {
    return data;
  }

  public void setData(AdsUpdateQueryData data)
  {
    this.data = data;
  }

  public static class AdsUpdateQueryData
  {
    private SimpleTimeBucket time;
    private AdsKeys keys;

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
  }
}
