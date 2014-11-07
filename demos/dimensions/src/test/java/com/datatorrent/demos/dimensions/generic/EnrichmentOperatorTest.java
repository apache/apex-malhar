/*
 * Copyright (c) 2014 DataTorrent, Inc. ALL Rights Reserved.
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
package com.datatorrent.demos.dimensions.generic;

import com.datatorrent.lib.testbench.CollectorTestSink;
import com.datatorrent.lib.util.TestUtils;
import com.esotericsoftware.kryo.Kryo;
import com.google.common.collect.Maps;
import org.junit.Assert;
import org.apache.commons.io.FileUtils;
import org.junit.Rule;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.util.Map;

public class EnrichmentOperatorTest
{
  @Rule
  public final TestUtils.TestInfo testInfo = new TestUtils.TestInfo();

  @Test
  public void testEnrichmentOperator() throws IOException, InterruptedException
  {
    URL origUrl = getClass().getResource("/productmapping.txt");

    URL fileUrl =  new URL(getClass().getResource("/").toString() + "productmapping1.txt");
    FileUtils.deleteQuietly(new File(fileUrl.getPath()));
    FileUtils.copyFile(new File(origUrl.getPath()), new File(fileUrl.getPath()));

    EnrichmentOperator oper = new EnrichmentOperator();
    oper.setFilePath(fileUrl.toString());
    oper.setLookupKey("productId");
    oper.setScanInterval(10);

    oper.setup(null);
    /* File contains 6 entries, but operator one entry is duplicate,
     * so cache should contains only 5 entries after scanning input file.
     */
    Assert.assertEquals("Number of mappings ", 7, oper.cache.size());

    CollectorTestSink<Map<String, Object>> sink = new CollectorTestSink<Map<String, Object>>();
    @SuppressWarnings({"unchecked", "rawtypes"})
    CollectorTestSink<Object> tmp = (CollectorTestSink) sink;
    oper.outputPort.setSink(tmp);

    oper.beginWindow(0);
    Map<String, Object> tuple = Maps.newHashMap();
    tuple.put("productId", 3);
    tuple.put("channelId", 4);
    tuple.put("amount", 10.0);

    Kryo kryo = new Kryo();
    oper.inputPort.process(kryo.copy(tuple));

    oper.endWindow();

    /* Number of tuple, emitted */
    Assert.assertEquals("Number of tuple emitted ", 1, sink.collectedTuples.size());
    Map<String, Object> emitted = sink.collectedTuples.iterator().next();

    /* The fields present in original event is kept as it is */
    Assert.assertEquals("Number of fields in emitted tuple", 4, emitted.size());
    Assert.assertEquals("value of productId is 3", tuple.get("productId"), emitted.get("productId"));
    Assert.assertEquals("value of channelId is 4", tuple.get("channelId"), emitted.get("channelId"));
    Assert.assertEquals("value of amount is 10.0", tuple.get("amount"), emitted.get("amount"));

    /* Check if productCategory is added to the event */
    Assert.assertEquals("productCategory is part of tuple", true, emitted.containsKey("productCategory"));
    Assert.assertEquals("value of product category is 1", 6, emitted.get("productCategory"));


    /* Check if modified file is reloaded in beginWindow */
    FileUtils.write(new File(fileUrl.getPath()), "{ \"productId\": 10, \"productCategory\": 5 }");
    Thread.sleep(100);
    oper.beginWindow(2);
    oper.endWindow();
    Assert.assertEquals("Number of mappings ", 1, oper.cache.size());
  }

  @Test
  public void testEnrichmentOperatorWithUpdateKeys() throws IOException, InterruptedException
  {
    URL origUrl = getClass().getResource("/productmapping.txt");

    URL fileUrl =  new URL(getClass().getResource("/").toString() + "productmapping1.txt");
    FileUtils.deleteQuietly(new File(fileUrl.getPath()));
    FileUtils.copyFile(new File(origUrl.getPath()), new File(fileUrl.getPath()));

    EnrichmentOperator oper = new EnrichmentOperator();
    oper.setFilePath(fileUrl.toString());
    oper.setLookupKey("productId");
    oper.setUpdateKeys("subCategory");
    oper.setScanInterval(10);

    oper.setup(null);
    /* File contains 6 entries, but operator one entry is duplicate,
     * so cache should contains only 5 entries after scanning input file.
     */
    Assert.assertEquals("Number of mappings ", 7, oper.cache.size());

    CollectorTestSink<Map<String, Object>> sink = new CollectorTestSink<Map<String, Object>>();
    @SuppressWarnings({"unchecked", "rawtypes"})
    CollectorTestSink<Object> tmp = (CollectorTestSink) sink;
    oper.outputPort.setSink(tmp);

    oper.beginWindow(0);
    Map<String, Object> tuple = Maps.newHashMap();
    tuple.put("productId", 7);
    tuple.put("channelId", 4);
    tuple.put("amount", 10.0);

    Kryo kryo = new Kryo();
    oper.inputPort.process(kryo.copy(tuple));

    oper.endWindow();

    /* Number of tuple, emitted */
    Assert.assertEquals("Number of tuple emitted ", 1, sink.collectedTuples.size());
    Map<String, Object> emitted = sink.collectedTuples.iterator().next();

    /* The fields present in original event is kept as it is */
    Assert.assertEquals("Number of fields in emitted tuple", 4, emitted.size());
    Assert.assertEquals("value of productId is 3", tuple.get("productId"), emitted.get("productId"));
    Assert.assertEquals("value of channelId is 4", tuple.get("channelId"), emitted.get("channelId"));
    Assert.assertEquals("value of amount is 10.0", tuple.get("amount"), emitted.get("amount"));

    /* Check if productCategory is added to the event */
    Assert.assertEquals("productCategory is not part of tuple", false, emitted.containsKey("productCategory"));
    Assert.assertEquals("subCategory is part of tuple", true, emitted.containsKey("subCategory"));
    Assert.assertEquals("value of product category is 1", 4, emitted.get("subCategory"));
  }


}
