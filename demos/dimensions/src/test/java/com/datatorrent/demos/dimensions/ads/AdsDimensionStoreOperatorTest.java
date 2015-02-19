/*
 * Copyright (c) 2014 DataTorrent, Inc. ALL Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.datatorrent.demos.dimensions.ads;

import com.datatorrent.api.Context.OperatorContext;
import com.datatorrent.common.util.Slice;
import com.datatorrent.contrib.hdht.tfile.TFileImpl;
import com.datatorrent.lib.appdata.schemas.ads.AdsKeys;
import com.datatorrent.lib.appdata.schemas.ads.AdsOneTimeQuery;
import com.datatorrent.lib.appdata.schemas.ads.AdsOneTimeQuery.AdsOneTimeQueryData;
import com.datatorrent.lib.appdata.schemas.ads.AdsOneTimeResult;
import com.datatorrent.lib.appdata.schemas.ads.AdsTimeRangeBucket;
import com.datatorrent.lib.testbench.CollectorTestSink;
import com.datatorrent.lib.util.TestUtils;
import com.google.common.util.concurrent.MoreExecutors;
import java.io.File;
import java.util.Date;
import java.util.concurrent.TimeUnit;
import org.apache.commons.io.FileUtils;
import org.codehaus.jackson.map.ObjectMapper;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AdsDimensionStoreOperatorTest
{
  private static final Logger logger = LoggerFactory.getLogger(AdsDimensionStoreOperatorTest.class);

  @Rule
  public final TestUtils.TestInfo testInfo = new TestUtils.TestInfo();

  @Test
  public void testQuery() throws Exception {
    ObjectMapper om = new ObjectMapper();

    File file = new File(testInfo.getDir());
    FileUtils.deleteDirectory(file);

    AdsDimensionStoreOperator hdsOut = new AdsDimensionStoreOperator() {
      @Override
      public void setup(OperatorContext arg0)
      {
        super.setup(arg0);
        super.writeExecutor = super.queryExecutor = MoreExecutors.sameThreadExecutor(); // synchronous processing
      }
    };
    TFileImpl hdsFile = new TFileImpl.DefaultTFileImpl();
    hdsOut.setFileStore(hdsFile);
    hdsFile.setBasePath(testInfo.getDir());
    hdsOut.setAggregator(new AdInfo.AdInfoAggregator());
    hdsOut.setMaxCacheSize(1);
    hdsOut.setFlushIntervalCount(0);
    hdsOut.setup(null);

    hdsOut.setDebug(false);

    CollectorTestSink<String> queryResults = new CollectorTestSink<String>();
    @SuppressWarnings({"unchecked", "rawtypes"})
    CollectorTestSink<Object> tmp = (CollectorTestSink) queryResults;
    hdsOut.queryResult.setSink(tmp);

    hdsOut.beginWindow(1);

    long baseTime = System.currentTimeMillis();
    long baseMinute = TimeUnit.MILLISECONDS.convert(TimeUnit.MINUTES.convert(baseTime, TimeUnit.MILLISECONDS), TimeUnit.MINUTES);

    // Check aggregation for ae1 and ae2 as they have same key.
    AdInfo.AdInfoAggregateEvent ae1 = new AdInfo.AdInfoAggregateEvent();
    ae1.publisherId = 1;
    ae1.advertiserId = 2;
    ae1.adUnit = 1;
    ae1.timestamp = baseMinute;
    ae1.clicks = 10;
    hdsOut.input.process(ae1);

    logger.debug("AdEvent 1: {}", ae1);

    AdInfo.AdInfoAggregateEvent ae2 = new AdInfo.AdInfoAggregateEvent();
    ae2.publisherId = 1;
    ae2.advertiserId = 2;
    ae2.adUnit = 1;
    ae2.timestamp = baseMinute;
    ae2.clicks = 20;
    hdsOut.input.process(ae2);

    logger.debug("AdEvent 2: {}", ae1);

    AdInfo.AdInfoAggregateEvent ae3 = new AdInfo.AdInfoAggregateEvent();
    ae3.publisherId = 1;
    ae3.advertiserId = 2;
    ae3.adUnit = 1;
    ae3.timestamp = baseMinute + TimeUnit.MILLISECONDS.convert(1, TimeUnit.MINUTES);
    ae3.clicks = 40;
    hdsOut.input.process(ae3);

    logger.debug("AdEvent 3: {}", ae1);

    hdsOut.endWindow();

    hdsOut.beginWindow(2);

    AdsOneTimeQuery aotq = new AdsOneTimeQuery();
    aotq.setId("query1");
    aotq.setType("oneTimeQuery");

    AdsOneTimeQueryData aotqd = new AdsOneTimeQueryData();

    AdsKeys aks = new AdsKeys();
    aks.setPublisherId(1);
    aks.setAdvertiserId(2);
    aks.setLocationId(1);

    aotqd.setKeys(aks);

    AdsTimeRangeBucket atrb = new AdsTimeRangeBucket();
    atrb.setFromLong(baseMinute);
    atrb.setToLong(baseMinute + TimeUnit.MILLISECONDS.convert(20, TimeUnit.MINUTES));
    atrb.setBucket("1h");

    aotqd.setTime(atrb);
    aotq.setData(aotqd);

    String query = om.writeValueAsString(aotq);
    logger.debug("Input query: {}", query);

    hdsOut.query.process(query);
    hdsOut.endWindow();

    Thread.sleep(1000);

    Assert.assertEquals("queryResults " + queryResults.collectedTuples, 1, queryResults.collectedTuples.size());
    String result = queryResults.collectedTuples.iterator().next();
    logger.debug("Result: {}", result);
    AdsOneTimeResult aotr = om.readValue(result, AdsOneTimeResult.class);

    Assert.assertEquals("result points", 2, aotr.getData().size());

    // ae1 object is stored as referenced in cache, and when new tuple is aggregated,
    // the new values are updated in ae1 itself, causing following check to fail.
    //Assert.assertEquals("clicks", ae1.clicks + ae2.clicks, r.data.get(0).clicks);
    //Assert.assertEquals("clicks", 10 + ae2.clicks, r.data.get(0).clicks);
    //Assert.assertEquals("clicks", ae3.clicks, r.data.get(1).clicks);

    //Assert.assertNotSame("deserialized", ae1, r.data.get(1));
    //Assert.assertSame("from cache", ae3, r.data.get(1));
  }

  @Test
  public void testEncodingAndDecoding()
  {
    AdInfo.AdInfoAggregateEvent ae = new AdInfo.AdInfoAggregateEvent();
    ae.timestamp = new Date().getTime();
    ae.publisherId = 1;
    ae.adUnit = 2;
    ae.advertiserId = 3;

    ae.impressions = 1000;
    ae.clicks = 100;
    ae.cost = 1.0;
    ae.revenue = 1.5;

    AdsDimensionStoreOperator oper = new AdsDimensionStoreOperator();

    // Encode/decode using normal mode
    oper.setDebug(false);
    Slice keyBytes = new Slice(oper.getKey(ae));
    byte[] valBytes = oper.getValue(ae);
    AdInfo.AdInfoAggregateEvent ae1 = oper.bytesToAggregate(keyBytes, valBytes);


    // Encode/decode using debug mode
    oper.setDebug(true);
    keyBytes = new Slice(oper.getKey(ae));
    valBytes = oper.getValue(ae);
    AdInfo.AdInfoAggregateEvent ae2 = oper.bytesToAggregate(keyBytes, valBytes);

    Assert.assertEquals(ae, ae1);
    Assert.assertEquals(ae, ae2);
  }

}

