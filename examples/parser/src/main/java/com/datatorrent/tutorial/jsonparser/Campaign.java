package com.datatorrent.tutorial.jsonparser;

import java.util.Date;

import com.fasterxml.jackson.annotation.JsonFormat;
import com.fasterxml.jackson.annotation.JsonProperty;

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
