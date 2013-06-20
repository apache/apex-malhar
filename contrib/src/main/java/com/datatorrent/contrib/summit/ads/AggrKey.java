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

  Integer publisherId;
  Integer advertiserId;
  Integer adUnit;

  public AggrKey() {
  }

  public AggrKey(Calendar timestamp, int timeSpec, Integer publisherId, Integer advertiserId, Integer adUnit) {
    super(timestamp, timeSpec);
    this.publisherId = publisherId;
    this.advertiserId = advertiserId;
    this.adUnit = adUnit;
  }

  public Integer getPublisherId()
  {
    return publisherId;
  }

  public void setPublisherId(Integer publisherId)
  {
    this.publisherId = publisherId;
  }

  public Integer getAdvertiserId()
  {
    return advertiserId;
  }

  public void setAdvertiserId(Integer advertiserId)
  {
    this.advertiserId = advertiserId;
  }

  public Integer getAdUnit()
  {
    return adUnit;
  }

  public void setAdUnit(Integer adUnit)
  {
    this.adUnit = adUnit;
  }

  @Override
  public int hashCode()
  {
    int key = 0;
    if (publisherId != null) {
      key |= (1 << 23);
      key |= (publisherId << 16);
    }
    if (advertiserId != null) {
      key |= (1 << 15);
      key |= (advertiserId << 8);
    }
    if (adUnit != null) {
      key |= (1 << 7);
      key |= adUnit;
    }
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
        equal = checkIntEqual(publisherId, aggrKey.getPublisherId())
                        && checkIntEqual(advertiserId, aggrKey.getAdvertiserId())
                        && checkIntEqual(adUnit, aggrKey.getAdUnit());
      }
    }
    return equal;
  }

  @Override
  public String toString()
  {
    StringBuilder sb = new StringBuilder(super.toString());
    if (publisherId != null) sb.append("|0:").append(publisherId);
    if (advertiserId != null) sb.append("|1:").append(advertiserId);
    if (adUnit != null) sb.append("|2:").append(adUnit);
    return sb.toString();
  }

  private boolean checkIntEqual( Integer a, Integer b ) {
    if ((a == null) && (b == null)) return true;
    if ((a != null) && a.equals(b)) return true;
    return false;
  }

}
