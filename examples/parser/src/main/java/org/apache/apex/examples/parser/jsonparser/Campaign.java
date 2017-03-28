/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.apex.examples.parser.jsonparser;

import java.util.Date;

import com.fasterxml.jackson.annotation.JsonFormat;
import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * @since 3.7.0
 */
public class Campaign
{
  private int adId;
  private String campaignName;
  @JsonProperty("budget")
  private double campaignBudget;
  private boolean weatherTargeting;
  @JsonFormat(shape = JsonFormat.Shape.STRING, pattern = "yyyy/MM/dd")
  private Date startDate;

  public int getAdId()
  {
    return adId;
  }

  public void setAdId(int adId)
  {
    this.adId = adId;
  }

  public String getCampaignName()
  {
    return campaignName;
  }

  public void setCampaignName(String campaignName)
  {
    this.campaignName = campaignName;
  }

  public double getCampaignBudget()
  {
    return campaignBudget;
  }

  public void setCampaignBudget(double campaignBudget)
  {
    this.campaignBudget = campaignBudget;
  }

  public boolean isWeatherTargeting()
  {
    return weatherTargeting;
  }

  public void setWeatherTargeting(boolean weatherTargeting)
  {
    this.weatherTargeting = weatherTargeting;
  }

  public Date getStartDate()
  {
    return startDate;
  }

  public void setStartDate(Date startDate)
  {
    this.startDate = startDate;
  }

  @Override
  public String toString()
  {
    return "Campaign [adId=" + adId + ", campaignName=" + campaignName + ", campaignBudget=" + campaignBudget
        + ", weatherTargeting=" + weatherTargeting + ", startDate=" + startDate + "]";
  }
}
