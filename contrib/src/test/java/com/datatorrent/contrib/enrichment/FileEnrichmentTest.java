package com.datatorrent.contrib.enrichment;

import com.datatorrent.lib.testbench.CollectorTestSink;
import com.datatorrent.lib.util.TestUtils;
import com.esotericsoftware.kryo.Kryo;
import com.google.common.collect.Maps;
import org.apache.commons.io.FileUtils;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.util.Map;


public class FileEnrichmentTest
{

  @Rule public final TestUtils.TestInfo testInfo = new TestUtils.TestInfo();

  @Test public void testEnrichmentOperator() throws IOException, InterruptedException
  {
    URL origUrl = this.getClass().getResource("/productmapping.txt");

    URL fileUrl = new URL(this.getClass().getResource("/").toString() + "productmapping1.txt");
    FileUtils.deleteQuietly(new File(fileUrl.getPath()));
    FileUtils.copyFile(new File(origUrl.getPath()), new File(fileUrl.getPath()));

    MapEnrichmentOperator oper = new MapEnrichmentOperator();
    FSLoader store = new FSLoader();
    store.setFileName(fileUrl.toString());
    oper.setLookupFieldsStr("productId");
    oper.setStore(store);

    oper.setup(null);

    /* File contains 6 entries, but operator one entry is duplicate,
     * so cache should contains only 5 entries after scanning input file.
     */
    //Assert.assertEquals("Number of mappings ", 7, oper.cache.size());

    CollectorTestSink<Map<String, Object>> sink = new CollectorTestSink<Map<String, Object>>();
    @SuppressWarnings({ "unchecked", "rawtypes" }) CollectorTestSink<Object> tmp = (CollectorTestSink) sink;
    oper.output.setSink(tmp);

    oper.beginWindow(0);
    Map<String, Object> tuple = Maps.newHashMap();
    tuple.put("productId", 3);
    tuple.put("channelId", 4);
    tuple.put("amount", 10.0);

    Kryo kryo = new Kryo();
    oper.input.process(kryo.copy(tuple));

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
    Assert.assertEquals("value of product category is 1", 5, emitted.get("productCategory"));

  }
}

