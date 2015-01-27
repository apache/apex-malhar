package com.datatorrent.demos.dimensions.generic;

import org.junit.Assert;
import org.junit.Test;

public class GenericDimensionComputationTest
{
  @Test
  public void test()
  {
    SchemaConverter converter = new SchemaConverter();
    converter.setEventSchemaJSON(GenericAggregateSerializerTest.TEST_SCHEMA_JSON);
    GenericDimensionComputation dc = new GenericDimensionComputation();
    dc.setSchema(converter.getEventSchema());
    dc.setup(null);

    Assert.assertEquals("Total number of aggregators ", 8, dc.getAggregators().length);
  }
}
