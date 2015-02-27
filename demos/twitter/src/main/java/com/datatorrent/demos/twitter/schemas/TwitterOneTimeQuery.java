/*
 *  Copyright (c) 2012-2015 Malhar, Inc.
 *  All Rights Reserved.
 */

package com.datatorrent.demos.twitter.schemas;

import com.datatorrent.lib.appdata.qr.QRType;
import com.datatorrent.lib.appdata.qr.Query;
import com.datatorrent.lib.appdata.qr.QueryDeserializerInfo;
import com.datatorrent.lib.appdata.qr.SimpleQueryDeserializer;

import java.util.List;

/**
 *
 * @author Timothy Farkas: tim@datatorrent.com
 */
@QRType(type=TwitterOneTimeQuery.TYPE)
@QueryDeserializerInfo(clazz=SimpleQueryDeserializer.class)
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
