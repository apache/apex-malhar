/*
 *  Copyright (c) 2012-2015 Malhar, Inc.
 *  All Rights Reserved.
 */

package com.datatorrent.lib.appdata.schemas;

/**
 *
 * @author Timothy Farkas: tim@datatorrent.com
 */
public class SchemaResult
{
  private String id;
  private String type;
  private SchemaData data;

  /**
   * @return the id
   */
  public String getId()
  {
    return id;
  }

  /**
   * @param id the id to set
   */
  public void setId(String id)
  {
    this.id = id;
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
   * @return the data
   */
  public SchemaData getData()
  {
    return data;
  }

  /**
   * @param data the data to set
   */
  public void setData(SchemaData data)
  {
    this.data = data;
  }
}
