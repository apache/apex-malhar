/*
 *  Copyright (c) 2012-2015 Malhar, Inc.
 *  All Rights Reserved.
 */

package com.datatorrent.lib.appdata.schemas.ads;

import java.util.List;

/**
 *
 * @author Timothy Farkas: tim@datatorrent.com
 */
public class KeyMultiValue
{
  private String name;
  private List<String> keyValues;

  public KeyMultiValue()
  {
  }

  /**
   * @return the name
   */
  public String getName()
  {
    return name;
  }

  /**
   * @param name the name to set
   */
  public void setName(String name)
  {
    this.name = name;
  }

  /**
   * @return the keyValues
   */
  public List<String> getKeyValues()
  {
    return keyValues;
  }

  /**
   * @param keyValues the keyValues to set
   */
  public void setKeyValues(List<String> keyValues)
  {
    this.keyValues = keyValues;
  }
}
