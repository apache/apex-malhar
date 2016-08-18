package com.demo.myapexapp;

import java.util.Date;

public class PojoEvent
{

  private int advId;
  private int campaignId;
  private String campaignName;
  private double campaignBudget;
  private Date startDate;
  private Date endDate;
  private String securityCode;
  private boolean weatherTargeting;
  private boolean optimized;
  private String parentCampaign;
  private Character weatherTargeted;
  private String valid;

  public int getAdvId()
  {
    return advId;
  }

  public void setAdvId(int AdId)
  {
    this.advId = advId;
  }

  public int getCampaignId()
  {
    return campaignId;
  }

  public void setCampaignId(int campaignId)
  {
    this.campaignId = campaignId;
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

  public Date getStartDate()
  {
    return startDate;
  }

  public void setStartDate(Date startDate)
  {
    this.startDate = startDate;
  }

  public Date getEndDate()
  {
    return endDate;
  }

  public void setEndDate(Date endDate)
  {
    this.endDate = endDate;
  }

  public String getSecurityCode()
  {
    return securityCode;
  }

  public void setSecurityCode(String securityCode)
  {
    this.securityCode = securityCode;
  }

  public boolean isWeatherTargeting()
  {
    return weatherTargeting;
  }

  public void setWeatherTargeting(boolean weatherTargeting)
  {
    this.weatherTargeting = weatherTargeting;
  }

  public boolean isOptimized()
  {
    return optimized;
  }

  public void setOptimized(boolean optimized)
  {
    this.optimized = optimized;
  }

  public String getParentCampaign()
  {
    return parentCampaign;
  }

  public void setParentCampaign(String parentCampaign)
  {
    this.parentCampaign = parentCampaign;
  }

  public Character getWeatherTargeted()
  {
    return weatherTargeted;
  }

  public void setWeatherTargeted(Character weatherTargeted)
  {
    this.weatherTargeted = weatherTargeted;
  }

  public String getValid()
  {
    return valid;
  }

  public void setValid(String valid)
  {
    this.valid = valid;
  }

}
