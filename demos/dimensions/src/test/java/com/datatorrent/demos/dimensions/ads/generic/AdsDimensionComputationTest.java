/*
 * Copyright (c) 2015 DataTorrent, Inc. ALL Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.datatorrent.demos.dimensions.ads.generic;

import com.datatorrent.contrib.dimensions.AppDataSingleSchemaDimensionStoreHDHT;
import com.datatorrent.contrib.hdht.tfile.TFileImpl;

import static com.datatorrent.demos.dimensions.ads.generic.ApplicationWithHDHT.*;

import com.datatorrent.lib.appdata.dimensions.AggregateEvent;
import com.datatorrent.lib.appdata.dimensions.AggregateEvent.EventKey;
import com.datatorrent.lib.appdata.dimensions.AggregatorStaticType;
import com.datatorrent.lib.appdata.schemas.SchemaUtils;
import com.datatorrent.lib.appdata.schemas.Type;
import com.datatorrent.lib.io.fs.AbstractFileOutputOperatorTest.FSTestWatcher;
import com.datatorrent.lib.util.TestUtils.TestInfo;
import com.google.common.collect.Maps;
import junit.framework.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.Set;


public class AdsDimensionComputationTest
{
  private static final Logger logger = LoggerFactory.getLogger(AdsDimensionComputationTest.class);

  @Rule public TestInfo testMeta = new FSTestWatcher();

  @Test
  public void countAggregatorTest()
  {
    final String advertiser = "mc donalds";
    final String publisher = "google";
    final String location = "SKY";

    final Map<String, Type> fieldToType = Maps.newHashMap();

    fieldToType.put("impressions", Type.LONG);
    fieldToType.put("clicks", Type.LONG);
    fieldToType.put("cost", Type.LONG);
    fieldToType.put("revenue", Type.LONG);

    AdsDimensionComputation adc = new AdsDimensionComputation();
    String eventSchema = SchemaUtils.jarResourceFileToString(EVENT_SCHEMA);
    adc.setEventSchemaJSON(eventSchema);
    adc.setup(null);

    AdInfo adInfo = new AdInfo();

    adInfo.setAdvertiser(advertiser);
    adInfo.setPublisher(publisher);
    adInfo.setLocation(location);

    adInfo.setClicks(1);
    adInfo.setCost(1.0);
    adInfo.setImpressions(1);
    adInfo.setRevenue(1.0);

    adc.processInputEvent(adInfo);

    AggregateEvent ae = null;

    Set<Map.Entry<EventKey, AggregateEvent>> entries = adc.cache.asMap().entrySet();

    for(Map.Entry<EventKey, AggregateEvent> entry: entries) {
      EventKey eventKey = entry.getKey();

      if(eventKey.getAggregatorID() == AggregatorStaticType.COUNT.ordinal()) {
        ae = entry.getValue();
        break;
      }
    }

    Assert.assertEquals("Invalid field to type map for count aggregates",
                        fieldToType,
                        ae.getAggregates().getFieldDescriptor().getFieldToType());

    AppDataSingleSchemaDimensionStoreHDHT store = new AppDataSingleSchemaDimensionStoreHDHT();

    String basePath = testMeta.getDir();
    TFileImpl hdsFile = new TFileImpl.DefaultTFileImpl();
    hdsFile.setBasePath(basePath);
    store.setFileStore(hdsFile);
    store.getAppDataFormatter().setContinuousFormatString("#.00");

    eventSchema = SchemaUtils.jarResourceFileToString(EVENT_SCHEMA);
    String dimensionalSchema = SchemaUtils.jarResourceFileToString(DIMENSIONAL_SCHEMA);

    store.setEventSchemaJSON(eventSchema);
    store.setDimensionalSchemaJSON(dimensionalSchema);

    store.setup(null);

    store.processEvent(ae);

    Map.Entry<EventKey, AggregateEvent> entry = store.cache.asMap().entrySet().iterator().next();


    Assert.assertEquals("Invalid field to type map for count aggregates",
                        fieldToType,
                        entry.getValue().getAggregates().getFieldDescriptor().getFieldToType());
  }
}
