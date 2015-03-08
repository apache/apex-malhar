/*
 *  Copyright (c) 2012-2015 Malhar, Inc.
 *  All Rights Reserved.
 */

package com.datatorrent.lib.appdata.schemas;

import com.datatorrent.lib.appdata.qr.Query;
import com.datatorrent.lib.appdata.qr.DataDeserializerInfo;
import com.datatorrent.lib.appdata.qr.DataType;
import com.datatorrent.lib.appdata.qr.DataValidatorInfo;
import com.datatorrent.lib.appdata.qr.SimpleDataDeserializer;
import com.datatorrent.lib.appdata.qr.SimpleDataValidator;

/**
 *
 * @author Timothy Farkas: tim@datatorrent.com
 */
@DataType(type=SchemaQuery.TYPE)
@DataDeserializerInfo(clazz=SimpleDataDeserializer.class)
@DataValidatorInfo(clazz=SimpleDataValidator.class)
public class SchemaQuery extends Query
{
  public static final String TYPE = "schemaQuery";

  public SchemaQuery()
  {
  }
}
