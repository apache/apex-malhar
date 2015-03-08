/*
 *  Copyright (c) 2012-2015 Malhar, Inc.
 *  All Rights Reserved.
 */

package com.datatorrent.demos.dimensions.schemas;

import com.datatorrent.lib.appdata.qr.DataType;
import com.datatorrent.lib.appdata.qr.Query;
import com.datatorrent.lib.appdata.qr.DataDeserializerInfo;
import com.datatorrent.lib.appdata.qr.DataValidatorInfo;
import com.datatorrent.lib.appdata.qr.SimpleDataDeserializer;
import com.google.common.collect.Lists;
import javax.validation.constraints.NotNull;

import java.util.List;

@DataType(type=AdsDataQuery.TYPE)
@DataDeserializerInfo(clazz=SimpleDataDeserializer.class)
@DataValidatorInfo(clazz=AdsDataQueryValidator.class)
public class AdsDataQuery extends Query
{
  public static final String TYPE = "dataQuery";

  @NotNull
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
    @NotNull
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
