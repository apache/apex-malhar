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

import java.util.List;

@QRType(type=AdsDataQuery.TYPE)
@QueryDeserializerInfo(clazz=SimpleQueryDeserializer.class)
@QueryValidatorInfo(clazz=SimpleQueryValidator.class)
public class AdsDataQuery extends Query
{
  public static final String TYPE = "dataQuery";

  private AdsDataQueryData data;
  private Long countdown;
  private Boolean incompleteResultOK;

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
   * @return the countdown
   */
  public Long getCountdown()
  {
    return countdown;
  }

  /**
   * @param countdown the countdown to set
   */
  public void setCountdown(Long countdown)
  {
    this.countdown = countdown;
  }

  /**
   * @return the incompleteResultOK
   */
  public Boolean getIncompleteResultOK()
  {
    return incompleteResultOK;
  }

  /**
   * @param incompleteResultOK the incompleteResultOK to set
   */
  public void setIncompleteResultOK(Boolean incompleteResultOK)
  {
    this.incompleteResultOK = incompleteResultOK;
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
