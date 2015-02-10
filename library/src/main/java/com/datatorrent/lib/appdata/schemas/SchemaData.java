/*
 *  Copyright (c) 2012-2015 Malhar, Inc.
 *  All Rights Reserved.
 */

package com.datatorrent.lib.appdata.schemas;

import java.util.List;

/**
 *
 * @author Timothy Farkas: tim@datatorrent.com
 */
public class SchemaData
{
  private String schemaType;
  private String schemaVersion;
  private List<SchemaDataValue> values;

  /**
   * @return the schemaType
   */
  public String getSchemaType()
  {
    return schemaType;
  }

  /**
   * @param schemaType the schemaType to set
   */
  public void setSchemaType(String schemaType)
  {
    this.schemaType = schemaType;
  }

  /**
   * @return the schemaVersion
   */
  public String getSchemaVersion()
  {
    return schemaVersion;
  }

  /**
   * @param schemaVersion the schemaVersion to set
   */
  public void setSchemaVersion(String schemaVersion)
  {
    this.schemaVersion = schemaVersion;
  }

  /**
   * @return the values
   */
  public List<SchemaDataValue> getValues()
  {
    return values;
  }

  /**
   * @param values the values to set
   */
  public void setValues(List<SchemaDataValue> values)
  {
    this.values = values;
  }

  public static class SchemaDataValue
  {
    private String name;
    private String type;

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
  }
}
