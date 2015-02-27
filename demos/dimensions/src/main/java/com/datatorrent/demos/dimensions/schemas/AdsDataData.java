/*
 *  Copyright (c) 2012-2015 Malhar, Inc.
 *  All Rights Reserved.
 */

package com.datatorrent.demos.dimensions.schemas;

/**
 *
 * @author Timothy Farkas: tim@datatorrent.com
 */
public class AdsDataData extends AdsTimeBucket
{
  private AdsDataValues values;

  public AdsDataData()
  {
  }

  /**
   * @return the values
   */
  public AdsDataValues getValues()
  {
    return values;
  }

  /**
   * @param values the values to set
   */
  public void setValues(AdsDataValues values)
  {
    this.values = values;
  }
}
