/*
 *  Copyright (c) 2012-2015 Malhar, Inc.
 *  All Rights Reserved.
 */

package com.datatorrent.lib.appdata.qr;

/**
 *
 * @author Timothy Farkas: tim@datatorrent.com
 */
public abstract class CustomDataDeserializer
{
  private Class<? extends Data> dataClazz;

  public abstract Data deserialize(String json, Object context);

  /**
   * @return the dataClazz
   */
  public Class<? extends Data> getDataClazz()
  {
    return dataClazz;
  }

  /**
   * @param dataClazz the dataClazz to set
   */
  public void setDataClazz(Class<? extends Data> dataClazz)
  {
    this.dataClazz = dataClazz;
  }
}
