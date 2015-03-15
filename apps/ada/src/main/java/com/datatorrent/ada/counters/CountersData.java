/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

package com.datatorrent.ada.counters;

import com.datatorrent.lib.appdata.qr.Data;
import javax.validation.constraints.NotNull;

/**
 *
 * @author Timothy Farkas: tim@datatorrent.com
 */
public class CountersData extends Data
{
  public static final String FIELD_USER = "user";
  public static final String FIELD_APP_NAME = "appName";
  public static final String FIELD_LOGICAL_OPERATOR_NAME = "logicalOperatorName";
  public static final String FIELD_VERSION = "version";

  @NotNull
  private String user;
  @NotNull
  private String appName;
  @NotNull
  private String logicalOperatorName;
  @NotNull
  private String version;

  public CountersData()
  {
  }

  /**
   * @return the appName
   */
  public String getAppName()
  {
    return appName;
  }

  /**
   * @param appName the appName to set
   */
  public void setAppName(String appName)
  {
    this.appName = appName;
  }

  /**
   * @return the logicalOperatorName
   */
  public String getLogicalOperatorName()
  {
    return logicalOperatorName;
  }

  /**
   * @param logicalOperatorName the logicalOperatorName to set
   */
  public void setLogicalOperatorName(String logicalOperatorName)
  {
    this.logicalOperatorName = logicalOperatorName;
  }

  /**
   * @return the user
   */
  public String getUser()
  {
    return user;
  }

  /**
   * @param user the user to set
   */
  public void setUser(String user)
  {
    this.user = user;
  }

  /**
   * @return the version
   */
  public String getVersion()
  {
    return version;
  }

  /**
   * @param version the version to set
   */
  public void setVersion(String version)
  {
    this.version = version;
  }
}
