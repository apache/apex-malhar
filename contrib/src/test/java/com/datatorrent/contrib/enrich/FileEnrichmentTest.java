/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package com.datatorrent.contrib.enrich;

import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.util.Arrays;
import java.util.Map;

import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;

import org.apache.commons.io.FileUtils;

import com.esotericsoftware.kryo.Kryo;
import com.google.common.collect.Maps;
import com.datatorrent.lib.testbench.CollectorTestSink;
import com.datatorrent.lib.util.TestUtils;

public class FileEnrichmentTest
{

  @Rule
  public final TestUtils.TestInfo testInfo = new TestUtils.TestInfo();

  @Test
  public void testEnrichmentOperator() throws IOException, InterruptedException
  {
    URL origUrl = this.getClass().getResource("/productmapping.txt");

    URL fileUrl = new URL(this.getClass().getResource("/").toString() + "productmapping1.txt");
    FileUtils.deleteQuietly(new File(fileUrl.getPath()));
    FileUtils.copyFile(new File(origUrl.getPath()), new File(fileUrl.getPath()));

    MapEnricher oper = new MapEnricher();
    FSLoader store = new FSLoader();
    store.setFileName(fileUrl.toString());
    oper.setLookupFields(Arrays.asList("productId"));
    oper.setIncludeFields(Arrays.asList("productCategory"));
    oper.setStore(store);

    oper.setup(null);

    /* File contains 6 entries, but operator one entry is duplicate,
     * so cache should contains only 5 entries after scanning input file.
     */
    //Assert.assertEquals("Number of mappings ", 7, oper.cache.size());

    CollectorTestSink<Map<String, Object>> sink = new CollectorTestSink<>();
    @SuppressWarnings({ "unchecked", "rawtypes" })
    CollectorTestSink<Object> tmp = (CollectorTestSink)sink;
    oper.output.setSink(tmp);

    oper.activate(null);

    oper.beginWindow(0);
    Map<String, Object> tuple = Maps.newHashMap();
    tuple.put("productId", 3);
    tuple.put("channelId", 4);
    tuple.put("amount", 10.0);

    Kryo kryo = new Kryo();
    oper.input.process(kryo.copy(tuple));

    oper.endWindow();

    oper.deactivate();

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
    Assert.assertEquals("value of product category is 1", 5, emitted.get("productCategory"));
    Assert.assertTrue(emitted.get("productCategory") instanceof Integer);
  }
}

