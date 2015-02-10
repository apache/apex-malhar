/*
 *  Copyright (c) 2012-2015 Malhar, Inc.
 *  All Rights Reserved.
 */

package com.datatorrent.lib.appdata.schemas;

import com.datatorrent.lib.appdata.Query;
import com.datatorrent.lib.appdata.QueryDeserializerInfo;
import com.datatorrent.lib.appdata.QuerySchemaInfo;
import com.datatorrent.lib.appdata.SimpleQueryDeserializer;
import java.util.Map;

/**
 *
 * @author Timothy Farkas: tim@datatorrent.com
 */
@QuerySchemaInfo(type=UpdateQuery.UPDATE_QUERY_TYPE)
@QueryDeserializerInfo(clazz=SimpleQueryDeserializer.class)
public class UpdateQuery extends Query
{
  public static final String UPDATE_QUERY_TYPE = "schemaUpdate";
  public static final String FIELD_DATA = "data";

  private UQData data;

  public UpdateQuery()
  {
  }

  public void setData(UQData data)
  {
    this.data = data;
  }

  public UQData getData()
  {
    return data;
  }

  @Override
  public String getType()
  {
    return UPDATE_QUERY_TYPE;
  }

  public static class UQData
  {
    private UQTime time;
    private Map<String, String> keys;

    /**
     * @return the time
     */
    public UQTime getTime()
    {
      return time;
    }

    /**
     * @param time the time to set
     */
    public void setTime(UQTime time)
    {
      this.time = time;
    }

    /**
     * @return the keys
     */
    public Map<String, String> getKeys()
    {
      return keys;
    }

    /**
     * @param keys the keys to set
     */
    public void setKeys(Map<String, String> keys)
    {
      this.keys = keys;
    }
  }

  public static class UQTime
  {
    private String bucket;

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
  }
}
