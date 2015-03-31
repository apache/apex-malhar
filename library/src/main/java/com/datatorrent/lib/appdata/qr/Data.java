/*
 *  Copyright (c) 2012-2015 Malhar, Inc.
 *  All Rights Reserved.
 */

package com.datatorrent.lib.appdata.qr;

import com.google.common.base.Preconditions;

/**
 *
 * @author Timothy Farkas: tim@datatorrent.com
 */
public class Data
{
  public static final String FIELD_TYPE = "type";

  private String type;

  public Data()
  {
  }

  public Data(String type)
  {
    Preconditions.checkNotNull(type);
    this.type = type;
  }

  /**
   * @return the type
   */
  public String getType()
  {
    return type;
  }

  /**
   * @param type the type to set
   */
  public void setType(String type)
  {
    this.type = type;
  }

}
