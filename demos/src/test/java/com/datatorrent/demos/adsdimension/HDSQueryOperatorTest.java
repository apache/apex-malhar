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
package com.datatorrent.demos.adsdimension;

import java.io.File;
import java.util.concurrent.TimeUnit;

import org.apache.commons.io.FileUtils;
import org.codehaus.jettison.json.JSONObject;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;

import com.datatorrent.api.Context.OperatorContext;
import com.datatorrent.contrib.hds.tfile.TFileImpl;
import com.datatorrent.demos.adsdimension.HDSQueryOperator.HDSRangeQueryResult;
import com.datatorrent.lib.testbench.CollectorTestSink;
import com.datatorrent.lib.util.TestUtils;
import com.google.common.util.concurrent.MoreExecutors;


public class HDSQueryOperatorTest
{
  @Rule
  public final TestUtils.TestInfo testInfo = new TestUtils.TestInfo();

  @Test
  public void testQuery() throws Exception {
    File file = new File(testInfo.getDir());
    FileUtils.deleteDirectory(file);

    HDSQueryOperator hdsOut = new HDSQueryOperator() {
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

    CollectorTestSink<HDSQueryOperator.HDSRangeQueryResult> queryResults = new CollectorTestSink<HDSQueryOperator.HDSRangeQueryResult>();
    @SuppressWarnings({"unchecked", "rawtypes"})
    CollectorTestSink<Object> tmp = (CollectorTestSink) queryResults;
    hdsOut.queryResult.setSink(tmp);

    hdsOut.beginWindow(1);

    long baseTime = System.currentTimeMillis();
    long baseMinute = TimeUnit.MILLISECONDS.convert(TimeUnit.MINUTES.convert(baseTime, TimeUnit.MICROSECONDS), TimeUnit.MINUTES);

    AdInfo.AdInfoAggregateEvent ae1 = new AdInfo.AdInfoAggregateEvent();
    ae1.publisherId = 1;
    ae1.timestamp = baseMinute;
    ae1.clicks = 10;
    hdsOut.input.process(ae1);

    AdInfo.AdInfoAggregateEvent ae2 = new AdInfo.AdInfoAggregateEvent();
    ae2.publisherId = 1;
    ae2.timestamp = baseMinute + TimeUnit.MILLISECONDS.convert(1, TimeUnit.MINUTES);
    ae2.clicks = 40;
    hdsOut.input.process(ae2);

    hdsOut.endWindow();

    hdsOut.beginWindow(2);

    JSONObject keys = new JSONObject();
    keys.put("publisherId", String.valueOf(1));

    JSONObject query = new JSONObject();
    query.put("numResults", "20");
    query.put("keys", keys);
    query.put("id", "query1");
    query.put("startTime", baseMinute);
    query.put("endTime", baseMinute + TimeUnit.MILLISECONDS.convert(20, TimeUnit.MINUTES));

    hdsOut.query.process(query.toString());

    Assert.assertEquals("rangeQueries " + hdsOut.rangeQueries, 1, hdsOut.rangeQueries.size());
    HDSQueryOperator.HDSRangeQuery aq = hdsOut.rangeQueries.values().iterator().next();
    Assert.assertEquals("numTimeUnits " + hdsOut.rangeQueries, baseMinute, aq.startTime);

    hdsOut.endWindow();

    Assert.assertEquals("queryResults " + queryResults.collectedTuples, 1, queryResults.collectedTuples.size());
    HDSRangeQueryResult r = queryResults.collectedTuples.iterator().next();
    Assert.assertEquals("result points " + r, 2, r.data.size());

    Assert.assertEquals("clicks", 10, r.data.get(0).clicks);
    Assert.assertEquals("clicks", 40, r.data.get(1).clicks);

    Assert.assertSame("from cache", ae2, r.data.get(1));

  }

}

