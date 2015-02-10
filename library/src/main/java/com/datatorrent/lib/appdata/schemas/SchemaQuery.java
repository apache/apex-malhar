/*
 *  Copyright (c) 2012-2015 Malhar, Inc.
 *  All Rights Reserved.
 */

package com.datatorrent.lib.appdata.schemas;

import com.datatorrent.lib.appdata.Query;
import com.datatorrent.lib.appdata.QueryDeserializerInfo;
import com.datatorrent.lib.appdata.QuerySchemaInfo;
import com.datatorrent.lib.appdata.SimpleQueryDeserializer;
import org.codehaus.jackson.map.ObjectMapper;

/**
 *
 * @author Timothy Farkas: tim@datatorrent.com
 */
@QuerySchemaInfo(type=SchemaQuery.SCHEMA_QUERY_TYPE)
@QueryDeserializerInfo(clazz=SimpleQueryDeserializer.class)
public class SchemaQuery extends Query
{
  public static final String SCHEMA_QUERY_TYPE = "schemaQuery";
  private ObjectMapper om = null;

  public SchemaQuery()
  {
  }

  @Override
  public String getType()
  {
    return FIELD_TYPE;
  }
}
