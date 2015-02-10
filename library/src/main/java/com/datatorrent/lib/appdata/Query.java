/*
 *  Copyright (c) 2012-2015 Malhar, Inc.
 *  All Rights Reserved.
 */

package com.datatorrent.lib.appdata;

/**
 *
 * @author Timothy Farkas: tim@datatorrent.com
 */
public abstract class Query
{
  public static final String FIELD_ID = "id";
  public static final String FIELD_TYPE = "type";

  private String id;

  public Query()
  {
  }

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
  public abstract String getType();

  /**
   * @param type the type to set
   */
  public void setType(String type)
  {
  }
}
