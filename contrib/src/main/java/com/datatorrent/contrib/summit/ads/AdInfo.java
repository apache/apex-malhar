/*
 *  Copyright (c) 2012-2013 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.datatorrent.contrib.summit.ads;

/**
 *
 * @author Pramod Immaneni <pramod@malhar-inc.com>
 */
public class AdInfo
{

  public static final int VIEW = 0;
  public static final int CLICK = (1 << 24);

  public static final int ADU_MASK = 0xff;
  public static final int PUB_MASK = (0xff << 8);
  public static final int ADV_MASK = (0xff << 16);

  int key;
  double value;
  long timestamp;

  public AdInfo() {
  }

  public AdInfo(int key, double value, long timestamp) {
    this.key = key;
    this.value = value;
    this.timestamp = timestamp;
  }

  public int getKey()
  {
    return key;
  }

  public void setKey(int key)
  {
    this.key = key;
  }

  public double getValue()
  {
    return value;
  }

  public void setValue(double value)
  {
    this.value = value;
  }

  public long getTimestamp()
  {
    return timestamp;
  }

  public void setTimestamp(long timestamp)
  {
    this.timestamp = timestamp;
  }

}
