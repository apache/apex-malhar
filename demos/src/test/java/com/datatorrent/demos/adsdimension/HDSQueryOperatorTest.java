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

import com.datatorrent.contrib.hds.tfile.TFileImpl;
import com.datatorrent.lib.testbench.CollectorTestSink;
import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import org.apache.commons.io.FileUtils;
import org.codehaus.jettison.json.JSONObject;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestWatcher;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.util.concurrent.TimeUnit;


public class HDSQueryOperatorTest
{
  @Rule
  public final TestUtils.TestInfo testInfo = new TestUtils.TestInfo();

  @Test
  public void testQuery() throws Exception {
    File file = new File(testInfo.getDir());
    FileUtils.deleteDirectory(file);

    HDSQueryOperator hdsOut = new HDSQueryOperator();
    TFileImpl hdsFile = new TFileImpl.DefaultTFileImpl();
    hdsOut.setFileStore(hdsFile);
    hdsFile.setBasePath(testInfo.getDir());
    hdsOut.setAggregator(new AdInfo.AdInfoAggregator());
    hdsOut.setMaxCacheSize(0);
    hdsOut.setup(null);
    hdsOut.setDebug(false);

    CollectorTestSink<HDSQueryOperator.HDSRangeQueryResult> queryResults = new CollectorTestSink<HDSQueryOperator.HDSRangeQueryResult>();
    @SuppressWarnings({"unchecked", "rawtypes"})
    CollectorTestSink<Object> tmp = (CollectorTestSink) queryResults;
    hdsOut.queryResult.setSink(tmp);

    hdsOut.beginWindow(1);

    long baseTime = System.currentTimeMillis();
    long baseMinute = TimeUnit.MILLISECONDS.convert(TimeUnit.MINUTES.convert(baseTime, TimeUnit.MICROSECONDS), TimeUnit.MINUTES);

    AdInfo.AdInfoAggregateEvent a = new AdInfo.AdInfoAggregateEvent();
    a.publisherId = 1;
    a.timestamp = baseMinute;
    a.clicks = 10;
    hdsOut.input.process(a);

    a = new AdInfo.AdInfoAggregateEvent();
    a.publisherId = 1;
    a.timestamp = baseMinute + TimeUnit.MILLISECONDS.convert(1, TimeUnit.MINUTES);
    a.clicks = 40;
    hdsOut.input.process(a);

    hdsOut.endWindow();

    hdsOut.beginWindow(2);

    JSONObject keys = new JSONObject();
    keys.put("publisherId", String.valueOf(1));

    JSONObject query = new JSONObject();
    query.put("numResults", "20");
    query.put("keys", keys);
    query.put("id", "query1");
    query.put("startTime", baseMinute);
    query.put("endTime", baseMinute);

    hdsOut.query.process(query.toString());

    Assert.assertEquals("rangeQueries " + hdsOut.rangeQueries, 1, hdsOut.rangeQueries.size());
    HDSQueryOperator.HDSRangeQuery aq = hdsOut.rangeQueries.values().iterator().next();
    Assert.assertEquals("numTimeUnits " + hdsOut.rangeQueries, 20, aq.numResults);

    hdsOut.processAllQueries();
    Thread.sleep(1000);
    hdsOut.endWindow();

    Assert.assertEquals("queryResults " + queryResults.collectedTuples, 1,
        queryResults.collectedTuples.size());

    Assert.assertEquals("clicks", 10, queryResults.collectedTuples.iterator().next().data.iterator().next().clicks);
  }

}


class TestUtils
{
  public static class TestInfo extends TestWatcher
  {
    public org.junit.runner.Description desc;

    public String getDir()
    {
      String methodName = desc.getMethodName();
      String className = desc.getClassName();
      return "target/" + className + "/" + methodName;
    }

    @Override
    protected void starting(org.junit.runner.Description description)
    {
      this.desc = description;
    }
  };

  /**
   * Clone object by serializing and deserializing using Kryo.
   * Note this is different from using {@link com.esotericsoftware.kryo.Kryo#copy(Object)}, which will attempt to also clone transient fields.
   * @param kryo
   * @param src
   * @return
   * @throws java.io.IOException
   */
  public static <T> T clone(Kryo kryo, T src) throws IOException
  {
    ByteArrayOutputStream bos = new ByteArrayOutputStream();
    Output output = new Output(bos);
    kryo.writeObject(output, src);
    output.close();
    Input input = new Input(bos.toByteArray());
    @SuppressWarnings("unchecked")
    Class<T> clazz = (Class<T>)src.getClass();
    return kryo.readObject(input, clazz);
  }
}

