/*
 *  Copyright (c) 2012-2015 Malhar, Inc.
 *  All Rights Reserved.
 */

package com.datatorrent.lib.appdata.schemas.ads;

import com.datatorrent.lib.appdata.Query;
import com.datatorrent.lib.appdata.Result;
import com.datatorrent.lib.appdata.ResultSerializerInfo;
import com.datatorrent.lib.appdata.SimpleResultSerializer;
import com.datatorrent.lib.appdata.schemas.KeyMultiValue;
import com.datatorrent.lib.appdata.schemas.SchemaValues;
import com.datatorrent.lib.appdata.schemas.TimeRangeBuckets;
import java.util.List;

/**
 *
 * @author Timothy Farkas: tim@datatorrent.com
 */
@ResultSerializerInfo(clazz=SimpleResultSerializer.class)
public class AdsSchemaResult extends Result
{
  private AdsSchemaData data;

  public AdsSchemaResult(Query query)
  {
    super(query);
  }

  /**
   * @return the data
   */
  public AdsSchemaData getData()
  {
    return data;
  }

  /**
   * @param data the data to set
   */
  public void setData(AdsSchemaData data)
  {
    this.data = data;
  }

  public static class AdsSchemaData
  {
    private TimeRangeBuckets timeBuckets;
    private List<KeyMultiValue> keys;
    private List<SchemaValues> values;

    /**
     * @return the timeBuckets
     */
    public TimeRangeBuckets getTimeBuckets()
    {
      return timeBuckets;
    }

    /**
     * @param timeBuckets the timeBuckets to set
     */
    public void setTimeBuckets(TimeRangeBuckets timeBuckets)
    {
      this.timeBuckets = timeBuckets;
    }

    /**
     * @return the keys
     */
    public List<KeyMultiValue> getKeys()
    {
      return keys;
    }

    /**
     * @param keys the keys to set
     */
    public void setKeys(List<KeyMultiValue> keys)
    {
      this.keys = keys;
    }

    /**
     * @return the values
     */
    public List<SchemaValues> getValues()
    {
      return values;
    }

    /**
     * @param values the values to set
     */
    public void setValues(List<SchemaValues> values)
    {
      this.values = values;
    }
  }
}
