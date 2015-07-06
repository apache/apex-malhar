/**
 * Copyright (c) 2015 DataTorrent, Inc.
 * All rights reserved.
 */
package com.datatorrent.demos.dimensions.ads.custom;

import java.util.concurrent.TimeUnit;

import junit.framework.Assert;

import org.junit.Test;

import com.datatorrent.lib.appdata.gpo.GPOMutable;
import com.datatorrent.lib.appdata.schemas.SchemaUtils;
import com.datatorrent.lib.appdata.schemas.TimeBucket;
import com.datatorrent.lib.dimensions.DimensionsDescriptor;
import com.datatorrent.lib.dimensions.DimensionsEvent.Aggregate;
import com.datatorrent.lib.testbench.CollectorTestSink;
import com.datatorrent.lib.util.TestUtils;

import com.datatorrent.demos.dimensions.ads.AdInfo.AdInfoAggregateEvent;
import com.datatorrent.demos.dimensions.ads.InputItemGenerator;
import com.datatorrent.demos.dimensions.ads.stats.AdsConverter;

import static com.datatorrent.demos.dimensions.ads.stats.AdsDimensionsDemoPerformant.EVENT_SCHEMA;

public class AdsConverterTest
{
  @Test
  public void simpleTest()
  {
    AdsConverter adsConverter = new AdsConverter();

    String eventSchema = SchemaUtils.jarResourceFileToString(EVENT_SCHEMA);

    String[] dimensionSpecs = new String[] {
      "time=" + TimeUnit.MINUTES,
      "time=" + TimeUnit.MINUTES + ":location",
      "time=" + TimeUnit.MINUTES + ":advertiser",
      "time=" + TimeUnit.MINUTES + ":publisher",
      "time=" + TimeUnit.MINUTES + ":advertiser:location",
      "time=" + TimeUnit.MINUTES + ":publisher:location",
      "time=" + TimeUnit.MINUTES + ":publisher:advertiser",
      "time=" + TimeUnit.MINUTES + ":publisher:advertiser:location"
    };

    AdInfoAggregateEvent aae = new AdInfoAggregateEvent();
    aae.time = 60;
    aae.timeBucket = TimeBucket.MINUTE.ordinal();
    aae.publisher = "google";
    aae.advertiser = "safeway";
    aae.location = "SKY";
    aae.impressions = 1L;
    aae.clicks = 1L;
    aae.cost = 1.0;
    aae.revenue = 1.0;
    aae.setDimensionsDescriptorID(dimensionSpecs.length - 1);

    adsConverter.setEventSchemaJSON(eventSchema);
    adsConverter.setDimensionSpecs(dimensionSpecs);

    CollectorTestSink<Aggregate> sink = new CollectorTestSink<Aggregate>();
    TestUtils.setSink(adsConverter.outputPort, sink);

    adsConverter.setup(null);
    adsConverter.beginWindow(0L);
    adsConverter.inputPort.put(aae);
    adsConverter.endWindow();

    Assert.assertEquals(1, sink.collectedTuples.size());

    Aggregate aggregate = sink.collectedTuples.get(0);

    GPOMutable key = aggregate.getKeys();

    Assert.assertEquals(aae.timeBucket, key.getFieldInt(DimensionsDescriptor.DIMENSION_TIME_BUCKET));
    Assert.assertEquals(aae.time, key.getFieldLong(DimensionsDescriptor.DIMENSION_TIME));
    Assert.assertEquals(aae.publisher, key.getFieldString(InputItemGenerator.PUBLISHER));
    Assert.assertEquals(aae.advertiser, key.getFieldString(InputItemGenerator.ADVERTISER));
    Assert.assertEquals(aae.location, key.getFieldString(InputItemGenerator.LOCATION));

    GPOMutable aggregates = aggregate.getAggregates();

    Assert.assertEquals(aae.impressions, aggregates.getFieldLong(InputItemGenerator.IMPRESSIONS));
    Assert.assertEquals(aae.clicks, aggregates.getFieldLong(InputItemGenerator.CLICKS));
    Assert.assertEquals(aae.revenue, aggregates.getFieldDouble(InputItemGenerator.REVENUE));
    Assert.assertEquals(aae.cost, aggregates.getFieldDouble(InputItemGenerator.COST));

    sink.collectedTuples.clear();

    aae.setDimensionsDescriptorID(2);

    adsConverter.beginWindow(1L);
    adsConverter.inputPort.put(aae);
    adsConverter.endWindow();

    Assert.assertEquals(1, sink.collectedTuples.size());
    aggregate = sink.collectedTuples.get(0);

    key = aggregate.getKeys();

    Assert.assertEquals(aae.timeBucket, key.getFieldInt(DimensionsDescriptor.DIMENSION_TIME_BUCKET));
    Assert.assertEquals(aae.time, key.getFieldLong(DimensionsDescriptor.DIMENSION_TIME));
    Assert.assertFalse(key.getFieldDescriptor().getFields().getFields().contains(InputItemGenerator.PUBLISHER));
    Assert.assertEquals(aae.advertiser, key.getFieldString(InputItemGenerator.ADVERTISER));
    Assert.assertFalse(key.getFieldDescriptor().getFields().getFields().contains(InputItemGenerator.LOCATION));

    aggregates = aggregate.getAggregates();

    Assert.assertEquals(aae.impressions, aggregates.getFieldLong(InputItemGenerator.IMPRESSIONS));
    Assert.assertEquals(aae.clicks, aggregates.getFieldLong(InputItemGenerator.CLICKS));
    Assert.assertEquals(aae.revenue, aggregates.getFieldDouble(InputItemGenerator.REVENUE));
    Assert.assertEquals(aae.cost, aggregates.getFieldDouble(InputItemGenerator.COST));
  }
}
