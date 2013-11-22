/*
 * Copyright (c) 2013 DataTorrent, Inc. ALL Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.datatorrent.demos.scalability;

/**
 * <p>AdInfo class.</p>
 *
 * @since 0.3.2
 */
public class AdInfo
{

  Integer publisherId;
  Integer advertiserId;
  Integer adUnit;
  boolean click;
  double value;
  long timestamp;

  public AdInfo() {
  }

  public AdInfo(Integer publisherId, Integer advertiserId, Integer adUnit, boolean click, double value, long timestamp) {
    this.publisherId = publisherId;
    this.advertiserId = advertiserId;
    this.adUnit = adUnit;
    this.click = click;
    this.value = value;
    this.timestamp = timestamp;
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

  public boolean isClick()
  {
    return click;
  }

  public void setClick(boolean click)
  {
    this.click = click;
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
