/*
 *  Copyright (c) 2012-2015 Malhar, Inc.
 *  All Rights Reserved.
 */

package com.datatorrent.lib.appdata.schemas;

import com.datatorrent.lib.appdata.qr.Query;
import com.datatorrent.lib.appdata.qr.Result;
import java.util.Map;

/**
 *
 * @author Timothy Farkas: tim@datatorrent.com
 */
public class TimeSeriesTabularUpdateResult extends Result
{
  private TimeSeriesTabularUpdateResultData data;

  public TimeSeriesTabularUpdateResult(Query query)
  {
    super(query);
  }

  /**
   * @return the data
   */
  public TimeSeriesTabularUpdateResultData getData()
  {
    return data;
  }

  /**
   * @param data the data to set
   */
  public void setData(TimeSeriesTabularUpdateResultData data)
  {
    this.data = data;
  }

  public static class TimeSeriesTabularUpdateResultData
  {
    private String time;
    private String bucket;
    private Map<String, String> values;

    /**
     * @return the time
     */
    public String getTime()
    {
      return time;
    }

    /**
     * @param time the time to set
     */
    public void setTime(String time)
    {
      this.time = time;
    }

    /**
     * @return the bucket
     */
    public String getBucket()
    {
      return bucket;
    }

    /**
     * @param bucket the bucket to set
     */
    public void setBucket(String bucket)
    {
      this.bucket = bucket;
    }

    /**
     * @return the values
     */
    public Map<String, String> getValues()
    {
      return values;
    }

    /**
     * @param values the values to set
     */
    public void setValues(Map<String, String> values)
    {
      this.values = values;
    }
  }
}
