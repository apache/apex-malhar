/*
 *  Copyright (c) 2012-2015 Malhar, Inc.
 *  All Rights Reserved.
 */

package com.datatorrent.lib.appdata.schemas;

import com.datatorrent.lib.appdata.dimensions.AggregatorUtils;
import com.datatorrent.lib.appdata.qr.DataSerializerFactory;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 *
 * @author Timothy Farkas: tim@datatorrent.com
 */
public class SchemaDimensionalTest
{
  private static final Logger logger = LoggerFactory.getLogger(SchemaDimensionalTest.class);

  public SchemaDimensionalTest()
  {
  }

  @Test
  public void additionalValueTest()
  {
    String eventSchemaJSON = SchemaUtils.jarResourceFileToString("adsGenericEventSchemaAdditional.json");

    DataSerializerFactory dsf = new DataSerializerFactory(new AppDataFormatter());
    SchemaDimensional schemaDimensional = new SchemaDimensional(new DimensionalEventSchema(eventSchemaJSON,
                                                                                           AggregatorUtils.DEFAULT_AGGREGATOR_INFO));

    SchemaQuery schemaQuery = new SchemaQuery();
    schemaQuery.setId("1");
    schemaQuery.setType(SchemaQuery.TYPE);

    SchemaResult result = new SchemaResult(schemaQuery, schemaDimensional);
    String resultSchema = dsf.serialize(result);

    logger.debug("{}", resultSchema);
  }
}
