/*
 *  Copyright (c) 2012-2015 Malhar, Inc.
 *  All Rights Reserved.
 */

package com.datatorrent.lib.appdata.qr;

import com.google.common.base.Preconditions;
import javax.validation.constraints.NotNull;

/**
 *
 * @author Timothy Farkas: tim@datatorrent.com
 */
public class Query
{
  public static final String FIELD_ID = "id";
  public static final String FIELD_TYPE = "type";

  @NotNull
  private String id;
  @NotNull
  private String type;

  public Query()
  {
  }

  public Query(String id,
               String type)
  {
    Preconditions.checkNotNull(id);
    Preconditions.checkNotNull(type);
    this.id = id;
    this.type = type;
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

  @Override
  public String toString()
  {
    return "Query{" + "id=" + id + ", type=" + type + '}';
  }
}
