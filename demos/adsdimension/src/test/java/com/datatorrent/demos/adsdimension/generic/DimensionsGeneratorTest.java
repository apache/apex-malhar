package com.datatorrent.demos.adsdimension.generic;

import com.google.common.collect.Lists;
import junit.framework.Assert;
import org.junit.Test;
import org.slf4j.Logger;

public class DimensionsGeneratorTest
{
  @Test
  public void testDimensionGenerator()
  {
    EventSchema schema = GenericAggregateSerializerTest.getEventSchema();
    DimensionsGenerator gen = new DimensionsGenerator(schema);
    GenericAggregator[] aggregators = gen.generateAggregators();

    Assert.assertEquals("total aggregators ", 8, aggregators.length);

    for(int i = 0; i < aggregators.length; i++)
    {
      LOG.info("aggregator {} dimension {}", i, aggregators[i].getDimension());
    }
  }

  @Test
  public void testDimensionGenerator1()
  {
    EventSchema schema = GenericAggregateSerializerTest.getEventSchema();
    schema.dimensions = Lists.newArrayList();
    schema.dimensions.add("time=MINUTES:publisherId");
    schema.dimensions.add("time=MINUTES:publisherId:advertiserId");
    DimensionsGenerator gen = new DimensionsGenerator(schema);
    GenericAggregator[] aggregators = gen.generateAggregators();

    Assert.assertEquals("total aggregators ", schema.dimensions.size(), aggregators.length);

    for(int i = 0; i < aggregators.length; i++)
    {
      Assert.assertEquals("aggregator " + i , aggregators[i].getDimension(), schema.dimensions.get(i));
      LOG.info("aggregator {} dimension {}", i, aggregators[i].getDimension());
    }
  }

  private static final Logger LOG = org.slf4j.LoggerFactory.getLogger(DimensionsGeneratorTest.class);
}
