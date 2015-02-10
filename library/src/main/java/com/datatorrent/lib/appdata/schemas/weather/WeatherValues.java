/*
 *  Copyright (c) 2012-2015 Malhar, Inc.
 *  All Rights Reserved.
 */

package com.datatorrent.lib.appdata.schemas.weather;

/**
 *
 * @author Timothy Farkas: tim@datatorrent.com
 */
public class WeatherValues
{
  private float tempurature;
  private float humidity;
  private float precipitation;

  /**
   * @return the tempurature
   */
  public float getTempurature()
  {
    return tempurature;
  }

  /**
   * @param tempurature the tempurature to set
   */
  public void setTempurature(float tempurature)
  {
    this.tempurature = tempurature;
  }

  /**
   * @return the humidity
   */
  public float getHumidity()
  {
    return humidity;
  }

  /**
   * @param humidity the humidity to set
   */
  public void setHumidity(float humidity)
  {
    this.humidity = humidity;
  }

  /**
   * @return the precipitation
   */
  public float getPrecipitation()
  {
    return precipitation;
  }

  /**
   * @param precipitation the precipitation to set
   */
  public void setPrecipitation(float precipitation)
  {
    this.precipitation = precipitation;
  }

}
