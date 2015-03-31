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
public class Query extends Data
{
  public static final String FIELD_ID = "id";

  @NotNull
  private String id;

  public Query()
  {
  }

  public Query(String id,
               String type)
  {
    super(type);
    Preconditions.checkNotNull(id);
    this.id = id;
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


  @Override
  public String toString()
  {
    return "Query{" + "id=" + id + ", type=" + getType() + '}';
  }
}
