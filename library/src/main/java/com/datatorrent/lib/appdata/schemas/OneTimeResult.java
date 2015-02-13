/*
 *  Copyright (c) 2012-2015 Malhar, Inc.
 *  All Rights Reserved.
 */

package com.datatorrent.lib.appdata.schemas;

import com.datatorrent.lib.appdata.qr.Query;
import com.datatorrent.lib.appdata.qr.Result;
import com.datatorrent.lib.appdata.qr.ResultSerializerInfo;
import com.datatorrent.lib.appdata.qr.SimpleResultSerializer;
import java.util.List;
import java.util.Map;

/**
 *
 * @author Timothy Farkas: tim@datatorrent.com
 */
@ResultSerializerInfo(clazz=SimpleResultSerializer.class)
public class OneTimeResult extends Result
{
  private OneTimeResultData data;

  public OneTimeResult(Query query)
  {
    super(query);
  }

  /**
   * @return the data
   */
  public OneTimeResultData getData()
  {
    return data;
  }

  /**
   * @param data the data to set
   */
  public void setData(OneTimeResultData data)
  {
    this.data = data;
  }

  public static class OneTimeResultData
  {
    private TimeRangeBuckets time;
    private Map<String, String> keys;
    private List<OneTimeResultDataData> data;

    public TimeRangeBuckets getTime()
    {
      return time;
    }

    public void setTime(TimeRangeBuckets time)
    {
      this.time = time;
    }

    public Map<String, String> getKeys()
    {
      return keys;
    }

    public void setKeys(Map<String, String> keys)
    {
      this.keys = keys;
    }

    /**
     * @return the data
     */
    public List<OneTimeResultDataData> getData()
    {
      return data;
    }

    /**
     * @param data the data to set
     */
    public void setData(List<OneTimeResultDataData> data)
    {
      this.data = data;
    }
  }

  public static class OneTimeResultDataData
  {
    private String time;
    private String bucket;
    private Map<String, String> values;

    public String getTime()
    {
      return time;
    }

    public void setTime(String time)
    {
      this.time = time;
    }

    public String getBucket()
    {
      return bucket;
    }

    public void setBucket(String bucket)
    {
      this.bucket = bucket;
    }

    public Map<String, String> getValues()
    {
      return values;
    }

    public void setValues(Map<String, String> values)
    {
      this.values = values;
    }
  }
}
