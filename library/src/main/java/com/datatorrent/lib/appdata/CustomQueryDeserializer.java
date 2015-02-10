/*
 *  Copyright (c) 2012-2015 Malhar, Inc.
 *  All Rights Reserved.
 */

package com.datatorrent.lib.appdata;

/**
 *
 * @author Timothy Farkas: tim@datatorrent.com
 */
public abstract class CustomQueryDeserializer
{
  private Class<? extends Query> queryClazz;

  public abstract Query deserialize(String json);

  /**
   * @return the queryClazz
   */
  public Class<? extends Query> getQueryClazz()
  {
    return queryClazz;
  }

  /**
   * @param queryClazz the queryClazz to set
   */
  public void setQueryClazz(Class<? extends Query> queryClazz)
  {
    this.queryClazz = queryClazz;
  }
}
