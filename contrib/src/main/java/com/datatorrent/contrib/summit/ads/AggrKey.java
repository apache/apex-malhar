/*
 *  Copyright (c) 2012-2013 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.datatorrent.contrib.summit.ads;

import com.datatorrent.lib.util.TimeBucketKey;
import java.util.Calendar;

/**
 *
 * @author Pramod Immaneni <pramod@malhar-inc.com>
 */
public class AggrKey extends TimeBucketKey
{
  int key;

  public AggrKey() {
  }

  public AggrKey(Calendar timestamp, int timeSpec, int key) {
    super(timestamp, timeSpec);
    this.key = key;
  }

  public int getKey()
  {
    return key;
  }

  public void setKey(int key)
  {
    this.key = key;
  }

  @Override
  public int hashCode()
  {
    return super.hashCode() ^ key;
  }

  @Override
  public boolean equals(Object obj)
  {
    boolean equal = false;
    if (obj instanceof AggrKey) {
      boolean checkEqual = super.equals(obj);
      if (checkEqual) {
        AggrKey aggrKey = (AggrKey)obj;
        checkEqual = (key == aggrKey.getKey());
        equal = checkEqual;
      }
    }
    return equal;
  }

  @Override
  public String toString()
  {
    int publisherId = key & AdInfo.PUB_MASK;
    int advertiserId = key & AdInfo.ADV_MASK;
    int adUnit = key & AdInfo.ADU_MASK;
    StringBuilder sb = new StringBuilder(super.toString());
    sb.append('|');
    sb.append("0:").append(publisherId);
    sb.append("|1:").append(advertiserId);
    sb.append("|2:").append(adUnit);
    return sb.toString(); 
  }




}
