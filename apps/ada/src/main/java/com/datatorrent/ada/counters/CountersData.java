/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

package com.datatorrent.ada.counters;

/**
 *
 * @author Timothy Farkas: tim@datatorrent.com
 */
public class CountersData
{
  private String applicationID;
  private String applicationName;
  private String operatorID;
  private String operatorName;

  public CountersData()
  {
  }

  /**
   * @return the applicationID
   */
  public String getApplicationID()
  {
    return applicationID;
  }

  /**
   * @param applicationID the applicationID to set
   */
  public void setApplicationID(String applicationID)
  {
    this.applicationID = applicationID;
  }

  /**
   * @return the applicationName
   */
  public String getApplicationName()
  {
    return applicationName;
  }

  /**
   * @param applicationName the applicationName to set
   */
  public void setApplicationName(String applicationName)
  {
    this.applicationName = applicationName;
  }

  /**
   * @return the operatorID
   */
  public String getOperatorID()
  {
    return operatorID;
  }

  /**
   * @param operatorID the operatorID to set
   */
  public void setOperatorID(String operatorID)
  {
    this.operatorID = operatorID;
  }

  /**
   * @return the operatorName
   */
  public String getOperatorName()
  {
    return operatorName;
  }

  /**
   * @param operatorName the operatorName to set
   */
  public void setOperatorName(String operatorName)
  {
    this.operatorName = operatorName;
  }
}
