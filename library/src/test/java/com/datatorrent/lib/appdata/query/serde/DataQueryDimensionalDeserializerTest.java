/**
 * Copyright (c) 2015 DataTorrent, Inc.
 * All rights reserved.
 */

package com.datatorrent.lib.appdata.query.serde;

import java.util.HashSet;


import com.google.common.collect.Sets;

import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestWatcher;
import org.junit.runner.Description;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datatorrent.lib.appdata.schemas.DataQueryDimensional;
import com.datatorrent.lib.appdata.schemas.DimensionalConfigurationSchema;
import com.datatorrent.lib.appdata.schemas.DimensionalSchema;
import com.datatorrent.lib.appdata.schemas.SchemaRegistrySingle;
import com.datatorrent.lib.appdata.schemas.SchemaUtils;
import com.datatorrent.lib.appdata.schemas.TimeBucket;
import com.datatorrent.lib.dimensions.aggregator.AggregatorRegistry;

public class DataQueryDimensionalDeserializerTest
{
  @Rule
  public DeserializerTestWatcher testMeta = new DeserializerTestWatcher();

  public static class DeserializerTestWatcher extends TestWatcher
  {
    private SchemaRegistrySingle schemaRegistry;

    @Override
    protected void starting(Description description)
    {
      DimensionalSchema schema = new DimensionalSchema(new DimensionalConfigurationSchema(SchemaUtils.jarResourceFileToString("adsGenericEventSchema.json"),
                                                                                          AggregatorRegistry.DEFAULT_AGGREGATOR_REGISTRY));
      schemaRegistry = new SchemaRegistrySingle();
      schemaRegistry.registerSchema(schema);
    }

    public SchemaRegistrySingle getSchemaRegistry()
    {
      return schemaRegistry;
    }
  }

  @BeforeClass
  public static void setup()
  {
    AggregatorRegistry.DEFAULT_AGGREGATOR_REGISTRY.setup();
  }

  @Test
  public void testSimpleQueryDeserialize() throws Exception
  {
    DataQueryDimensional dqd = getDataQueryDimensional("dimensionalDataQuery.json");
    validateDataQueryDimensional(dqd);
    Assert.assertEquals(10, dqd.getLatestNumBuckets());
  }

  @Test
  public void testSimpleQueryDeserializeFromTo() throws Exception
  {
    DataQueryDimensional dqd = getDataQueryDimensional("dimensionalDataQueryFromTo.json");
    validateDataQueryDimensionalFromTo(dqd);
  }

  @Test
  public void testDefaultSlidingAggregateSize() throws Exception
  {
    DataQueryDimensional dqd = getDataQueryDimensional("dimensionalDataQuery.json");
    validateDataQueryDimensional(dqd);
    Assert.assertEquals(1, dqd.getSlidingAggregateSize());
  }

  @Test
  public void testSlidingAggregateSize() throws Exception
  {
    DataQueryDimensional dqd = getDataQueryDimensional("dimensionalDataQuerySlidingAggregateSize.json");
    validateDataQueryDimensional(dqd);
    Assert.assertEquals(10, dqd.getSlidingAggregateSize());
  }

  private DataQueryDimensional getDataQueryDimensional(String jsonFile) throws Exception
  {
    DataQueryDimensionalDeserializer dqdd = new DataQueryDimensionalDeserializer();
    String json = SchemaUtils.jarResourceFileToString(jsonFile);

    return (DataQueryDimensional) dqdd.deserialize(json, DataQueryDimensional.class, testMeta.getSchemaRegistry());
  }

  private void validateDataQueryDimensional(DataQueryDimensional dataQueryDimensional)
  {
    Assert.assertEquals("1", dataQueryDimensional.getId());
    Assert.assertEquals(TimeBucket.MINUTE, dataQueryDimensional.getTimeBucket());
    Assert.assertEquals(true, dataQueryDimensional.getIncompleteResultOK());
    Assert.assertEquals(new HashSet<String>(), dataQueryDimensional.getKeyFields().getFields());
    Assert.assertEquals(Sets.newHashSet("tax", "sales", "discount"),
                        dataQueryDimensional.getFieldsAggregatable().getAggregatorToFields().get("SUM"));
    Assert.assertEquals(Sets.newHashSet("time", "channel", "region", "product"),
                        dataQueryDimensional.getFieldsAggregatable().getNonAggregatedFields().getFields());
  }

  private void validateDataQueryDimensionalFromTo(DataQueryDimensional dataQueryDimensional)
  {
    validateDataQueryDimensional(dataQueryDimensional);

    Assert.assertEquals(1442698142862L, dataQueryDimensional.getFrom());
    Assert.assertEquals(1442698742862L, dataQueryDimensional.getTo());
  }

  private static final Logger LOG = LoggerFactory.getLogger(DataQueryDimensionalDeserializerTest.class);
}
