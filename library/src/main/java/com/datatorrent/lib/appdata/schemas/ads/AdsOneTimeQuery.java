/*
 *  Copyright (c) 2012-2015 Malhar, Inc.
 *  All Rights Reserved.
 */

package com.datatorrent.lib.appdata.schemas.ads;

import com.datatorrent.lib.appdata.qr.QRType;
import com.datatorrent.lib.appdata.qr.Query;
import com.datatorrent.lib.appdata.qr.QueryDeserializerInfo;
import com.datatorrent.lib.appdata.qr.QueryValidatorInfo;
import com.datatorrent.lib.appdata.qr.SimpleQueryDeserializer;
import com.datatorrent.lib.appdata.qr.SimpleQueryValidator;
import java.util.List;
import org.codehaus.jackson.map.annotate.JsonSerialize;

@QRType(type=AdsOneTimeQuery.TYPE)
@QueryDeserializerInfo(clazz=SimpleQueryDeserializer.class)
@QueryValidatorInfo(clazz=SimpleQueryValidator.class)
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
    private AdsTimeRangeBucket time;
    private AdsKeys keys;
    @JsonSerialize(include=JsonSerialize.Inclusion.NON_NULL)
    private List<String> fields;

    public AdsOneTimeQueryData()
    {
    }

    /**
     * @return the timeRangeBucket
     */
    public AdsTimeRangeBucket getTime()
    {
      return time;
    }

    /**
     * @param time the timeRangeBucket to set
     */
    public void setTime(AdsTimeRangeBucket time)
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
