/*
 *  Copyright (c) 2012-2015 Malhar, Inc.
 *  All Rights Reserved.
 */

package com.datatorrent.lib.appdata.schemas.twitter;

import com.datatorrent.lib.appdata.qr.QRType;
import com.datatorrent.lib.appdata.qr.Query;
import com.datatorrent.lib.appdata.qr.Result;
import com.datatorrent.lib.appdata.qr.ResultSerializerInfo;
import com.datatorrent.lib.appdata.qr.SimpleResultSerializer;
import java.util.List;

/**
 *
 * @author Timothy Farkas: tim@datatorrent.com
 */
@QRType(type=TwitterUpdateResult.TYPE)
@ResultSerializerInfo(clazz=SimpleResultSerializer.class)
public class TwitterUpdateResult extends Result
{
  public static final String TYPE = "updateData";

  private int countdown;
  private TwitterData data;

  public TwitterUpdateResult(Query query)
  {
    super(query);
  }

  /**
   * @return the countdown
   */
  public int getCountdown()
  {
    return countdown;
  }

  /**
   * @param countdown the countdown to set
   */
  public void setCountdown(int countdown)
  {
    this.countdown = countdown;
  }

  /**
   * @return the data
   */
  public TwitterData getData()
  {
    return data;
  }

  /**
   * @param data the data to set
   */
  public void setData(TwitterData data)
  {
    this.data = data;
  }

  public static class TwitterData
  {
    private List<TwitterDataValues> values;

    /**
     * @return the values
     */
    public List<TwitterDataValues> getValues()
    {
      return values;
    }

    /**
     * @param values the values to set
     */
    public void setValues(List<TwitterDataValues> values)
    {
      this.values = values;
    }
  }
}
