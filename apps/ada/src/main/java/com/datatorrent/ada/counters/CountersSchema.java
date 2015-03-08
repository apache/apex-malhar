/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

package com.datatorrent.ada.counters;


import java.util.List;
import java.util.Map;

/**
 *
 * @author Timothy Farkas: tim@datatorrent.com
 */
public class CountersSchema
{
  private String type;
  private String user;
  private String appName;
  private String logicalOperatorName;

  private Map<String, String> keys;
  private List<CountersSchemaValues> values;

  public CountersSchema()
  {
  }

  /**
   * @return the type
   */
  public String getType()
  {
    return type;
  }

  /**
   * @param type the type to set
   */
  public void setType(String type)
  {
    this.type = type;
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
   * @return the keys
   */
  public Map<String, String> getKeys()
  {
    return keys;
  }

  /**
   * @param keys the keys to set
   */
  public void setKeys(Map<String, String> keys)
  {
    this.keys = keys;
  }

  /**
   * @return the values
   */
  public List<CountersSchemaValues> getValues()
  {
    return values;
  }

  /**
   * @param values the values to set
   */
  public void setValues(List<CountersSchemaValues> values)
  {
    this.values = values;
  }

  public static class CountersSchemaValues
  {
    private String name;
    private String type;
    private List<String> aggregations;
    private List<String> dimensions;

    public CountersSchemaValues()
    {
    }

    /**
     * @return the name
     */
    public String getName()
    {
      return name;
    }

    /**
     * @param name the name to set
     */
    public void setName(String name)
    {
      this.name = name;
    }

    /**
     * @return the type
     */
    public String getType()
    {
      return type;
    }

    /**
     * @param type the type to set
     */
    public void setType(String type)
    {
      this.type = type;
    }

    /**
     * @return the aggregations
     */
    public List<String> getAggregations()
    {
      return aggregations;
    }

    /**
     * @param aggregations the aggregations to set
     */
    public void setAggregations(List<String> aggregations)
    {
      this.aggregations = aggregations;
    }

    /**
     * @return the dimensions
     */
    public List<String> getDimensions()
    {
      return dimensions;
    }

    /**
     * @param dimensions the dimensions to set
     */
    public void setDimensions(List<String> dimensions)
    {
      this.dimensions = dimensions;
    }
  }
}
