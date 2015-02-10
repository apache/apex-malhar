/*
 *  Copyright (c) 2012-2015 Malhar, Inc.
 *  All Rights Reserved.
 */

package com.datatorrent.lib.appdata.schemas;

/**
 *
 * @author Timothy Farkas: tim@datatorrent.com
 */
public class SchemaData
{
  private String schemaType;
  private String schemaVersion;

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
}
