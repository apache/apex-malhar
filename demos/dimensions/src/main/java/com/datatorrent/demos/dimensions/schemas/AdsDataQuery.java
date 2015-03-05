/*
 *  Copyright (c) 2012-2015 Malhar, Inc.
 *  All Rights Reserved.
 */

package com.datatorrent.demos.dimensions.schemas;

import com.datatorrent.lib.appdata.qr.QRType;
import com.datatorrent.lib.appdata.qr.Query;
import com.datatorrent.lib.appdata.qr.QueryDeserializerInfo;
import com.datatorrent.lib.appdata.qr.QueryValidatorInfo;
import com.datatorrent.lib.appdata.qr.SimpleQueryDeserializer;
import com.datatorrent.lib.appdata.qr.SimpleQueryValidator;
import com.google.common.collect.Lists;

import java.util.List;

@QRType(type=AdsDataQuery.TYPE)
@QueryDeserializerInfo(clazz=SimpleQueryDeserializer.class)
@QueryValidatorInfo(clazz=SimpleQueryValidator.class)
public class AdsDataQuery extends Query
{
  public static final String TYPE = "dataQuery";

  private AdsDataQueryData data;
  private Long countdown = 30L;
  private boolean incompleteResultOK = true;

  public AdsDataQuery()
  {
  }

  public AdsDataQueryData getData()
  {
    return data;
  }

  public void setData(AdsDataQueryData data)
  {
    this.data = data;
  }

  /**
   * @return the incompleteResultOK
   */
  public boolean getIncompleteResultOK()
  {
    return incompleteResultOK;
  }

  /**
   * @param incompleteResultOK the incompleteResultOK to set
   */
  public void setIncompleteResultOK(boolean incompleteResultOK)
  {
    this.incompleteResultOK = incompleteResultOK;
  }

  /**
   * @return the countdown
   */
  public long getCountdown()
  {
    return countdown;
  }

  /**
   * @param countdown the countdown to set
   */
  public void setCountdown(long countdown)
  {
    this.countdown = countdown;
  }

  public static class AdsDataQueryData
  {
    private AdsTimeRangeBucket time;
    private AdsKeys keys;
    private List<String> fields;

    /**
     * @return the time
     */
    public AdsTimeRangeBucket getTime()
    {
      return time;
    }

    /**
     * @param time the time to set
     */
    public void setTime(AdsTimeRangeBucket time)
    {
      this.time = time;
    }

    /**
     * @return the keys
     */
    public AdsKeys getKeys()
    {
      if(keys == null) {
        keys = new AdsKeys();
      }

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
     * @return the fields
     */
    public List<String> getFields()
    {
      if(fields == null) {
        fields = Lists.newArrayList();
      }

      return fields;
    }

    /**
     * @param fields the fields to set
     */
    public void setFields(List<String> fields)
    {
      this.fields = fields;
    }
  }
}
