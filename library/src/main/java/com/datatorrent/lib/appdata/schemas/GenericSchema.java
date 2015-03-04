/*
 *  Copyright (c) 2012-2015 Malhar, Inc.
 *  All Rights Reserved.
 */

package com.datatorrent.lib.appdata.schemas;

/**
 *
 * @author Timothy Farkas: tim@datatorrent.com
 */
public interface GenericSchema
{
  public String getSchemaType();
  public String getSchemaVersion();
  public String getSchemaJSON();
}
