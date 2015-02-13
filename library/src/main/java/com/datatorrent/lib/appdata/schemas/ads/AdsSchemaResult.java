/*
 *  Copyright (c) 2012-2015 Malhar, Inc.
 *  All Rights Reserved.
 */

package com.datatorrent.lib.appdata.schemas.ads;

import com.datatorrent.lib.appdata.qr.QRType;
import com.datatorrent.lib.appdata.qr.Query;
import com.datatorrent.lib.appdata.qr.Result;
import com.datatorrent.lib.appdata.qr.ResultSerializerInfo;
import com.datatorrent.lib.appdata.qr.SimpleResultSerializer;
import com.datatorrent.lib.appdata.schemas.KeyMultiValue;
import com.datatorrent.lib.appdata.schemas.SchemaValues;
import com.datatorrent.lib.appdata.schemas.TimeRangeIntervals;
import java.util.List;

/**
 *
 * @author Timothy Farkas: tim@datatorrent.com
 */

@QRType(type=AdsSchemaResult.TYPE)
@ResultSerializerInfo(clazz=SimpleResultSerializer.class)
public class AdsSchemaResult extends Result
{
  public static final String TYPE = "schemaData";

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
    private String schemaType;
    private String schemaVersion;

    private TimeRangeIntervals timeBuckets;
    private List<KeyMultiValue> keys;
    private List<SchemaValues> values;

    /**
     * @return the timeBuckets
     */
    public TimeRangeIntervals getTimeBuckets()
    {
      return timeBuckets;
    }

    /**
     * @param timeBuckets the timeBuckets to set
     */
    public void setTimeBuckets(TimeRangeIntervals timeBuckets)
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

    /**
     * @return the schemaType
     */
    public String getSchemaType()
    {
      return schemaType;
    }

    /**
     * @param schemaType the schemaType to set
     */
    public void setSchemaType(String schemaType)
    {
      this.schemaType = schemaType;
    }

    /**
     * @return the schemaVersion
     */
    public String getSchemaVersion()
    {
      return schemaVersion;
    }

    /**
     * @param schemaVersion the schemaVersion to set
     */
    public void setSchemaVersion(String schemaVersion)
    {
      this.schemaVersion = schemaVersion;
    }
  }
}
