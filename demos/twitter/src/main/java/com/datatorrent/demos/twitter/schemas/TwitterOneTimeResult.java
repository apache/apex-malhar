/*
 *  Copyright (c) 2012-2015 Malhar, Inc.
 *  All Rights Reserved.
 */

package com.datatorrent.demos.twitter.schemas;

import com.datatorrent.lib.appdata.qr.DataType;
import com.datatorrent.lib.appdata.qr.Query;
import com.datatorrent.lib.appdata.qr.Result;
import com.datatorrent.lib.appdata.qr.DataSerializerInfo;
import com.datatorrent.lib.appdata.qr.SimpleDataSerializer;

import java.util.List;

/**
 *
 * @author Timothy Farkas: tim@datatorrent.com
 */
@DataType(type=TwitterOneTimeResult.TYPE)
@DataSerializerInfo(clazz=SimpleDataSerializer.class)
public class TwitterOneTimeResult extends Result
{
  public static final String TYPE = "oneTimeData";

  private TwitterData data;

  public TwitterOneTimeResult(Query query)
  {
    super(query);
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
