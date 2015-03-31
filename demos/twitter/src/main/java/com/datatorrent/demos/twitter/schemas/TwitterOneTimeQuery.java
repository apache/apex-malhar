/*
 *  Copyright (c) 2012-2015 Malhar, Inc.
 *  All Rights Reserved.
 */

package com.datatorrent.demos.twitter.schemas;

import com.datatorrent.lib.appdata.qr.DataType;
import com.datatorrent.lib.appdata.qr.Query;
import com.datatorrent.lib.appdata.qr.DataDeserializerInfo;
import com.datatorrent.lib.appdata.qr.SimpleDataDeserializer;

import java.util.List;

/**
 *
 * @author Timothy Farkas: tim@datatorrent.com
 */
@DataType(type=TwitterOneTimeQuery.TYPE)
@DataDeserializerInfo(clazz=SimpleDataDeserializer.class)
public class TwitterOneTimeQuery extends Query
{
  public static final String TYPE = "oneTimeQuery";

  private List<String> fields;

  public TwitterOneTimeQuery()
  {
  }

  /**
   * @return the fields
   */
  public List<String> getFields()
  {
    return fields;
  }

  /**
   * @param fields the fields to set
   */
  public void setFields(List<String> fields)
  {
    this.fields = fields;
  }
}
