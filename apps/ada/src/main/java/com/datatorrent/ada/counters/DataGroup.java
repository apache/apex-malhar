/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

package com.datatorrent.ada.counters;

import com.google.common.base.Preconditions;

/**
 *
 * @author Timothy Farkas: tim@datatorrent.com
 */
public class DataGroup
{
  private String user;
  private String appName;
  private String logicalOperatorName;
  private String version;

  public DataGroup(DataGroup dataGroup)
  {
    this.user = dataGroup.getUser();
    this.appName = dataGroup.getAppName();
    this.logicalOperatorName = dataGroup.getLogicalOperatorName();
    this.version = dataGroup.getVersion();
  }

  public DataGroup(String user,
                   String appName,
                   String logicalOperatorName,
                   String version)
  {
    setUser(user);
    setAppName(appName);
    setLogicalOperatorName(logicalOperatorName);
    setVersion(version);
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
  private void setUser(String user)
  {
    Preconditions.checkNotNull(user);
    this.user = user;
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
  private void setAppName(String appName)
  {
    Preconditions.checkNotNull(appName);
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
  private void setLogicalOperatorName(String logicalOperatorName)
  {
    Preconditions.checkNotNull(logicalOperatorName);
    this.logicalOperatorName = logicalOperatorName;
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
  private void setVersion(String version)
  {
    Preconditions.checkNotNull(version);
    this.version = version;
  }

  @Override
  public int hashCode()
  {
    int hash = 7;
    hash = 31 * hash + (this.user != null ? this.user.hashCode() : 0);
    hash = 31 * hash + (this.appName != null ? this.appName.hashCode() : 0);
    hash = 31 * hash + (this.logicalOperatorName != null ? this.logicalOperatorName.hashCode() : 0);
    hash = 31 * hash + (this.version != null ? this.version.hashCode() : 0);
    return hash;
  }

  @Override
  public boolean equals(Object obj)
  {
    if(obj == null) {
      return false;
    }
    if(getClass() != obj.getClass()) {
      return false;
    }
    final DataGroup other = (DataGroup)obj;
    if((this.user == null) ? (other.user != null) : !this.user.equals(other.user)) {
      return false;
    }
    if((this.appName == null) ? (other.appName != null) : !this.appName.equals(other.appName)) {
      return false;
    }
    if((this.logicalOperatorName == null) ? (other.logicalOperatorName != null) : !this.logicalOperatorName.equals(other.logicalOperatorName)) {
      return false;
    }
    if((this.version == null) ? (other.version != null) : !this.version.equals(other.version)) {
      return false;
    }
    return true;
  }
}
