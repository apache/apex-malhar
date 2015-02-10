/*
 *  Copyright (c) 2012-2015 Malhar, Inc.
 *  All Rights Reserved.
 */

package com.datatorrent.lib.appdata.schemas.ads;

import com.datatorrent.lib.appdata.Query;
import com.datatorrent.lib.appdata.Result;
import com.datatorrent.lib.appdata.ResultSerializerInfo;
import com.datatorrent.lib.appdata.SimpleResultSerializer;

/**
 *
 * @author Timothy Farkas: tim@datatorrent.com
 */
@ResultSerializerInfo(clazz=SimpleResultSerializer.class)
public class AdsUpdateResult extends Result
{
  private AdsUpdateData data;

  public AdsUpdateResult(Query query)
  {
    super(query);
  }

  /**
   * @return the data
   */
  public AdsUpdateData getData()
  {
    return data;
  }

  /**
   * @param data the data to set
   */
  public void setData(AdsUpdateData data)
  {
    this.data = data;
  }

  public static class AdsUpdateData
  {
    private AdsKeys keys;
    private AdsDataData data;

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
    public AdsDataData getData()
    {
      return data;
    }

    /**
     * @param data the data to set
     */
    public void setData(AdsDataData data)
    {
      this.data = data;
    }
  }
}
