/**
 * Copyright (c) 2015 DataTorrent, Inc.
 * All rights reserved.
 */
package com.datatorrent.demos.dimensions.ads.generic;

import java.net.URI;

import org.codehaus.jettison.json.JSONArray;
import org.codehaus.jettison.json.JSONObject;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.conf.Configuration;

import com.datatorrent.lib.appdata.schemas.SchemaUtils;
import com.datatorrent.lib.io.PubSubWebSocketAppDataQuery;
import com.datatorrent.lib.io.PubSubWebSocketOutputOperator;
import com.datatorrent.lib.testbench.CollectorTestSink;
import com.datatorrent.lib.util.TestUtils;
import com.datatorrent.lib.util.TestUtils.TestInfo;

import com.datatorrent.api.LocalMode;

/**
 * This test requires a gateway running on the local machine.
 */
public class AdsDimensionsDemoTest
{
  @Rule
  public TestInfo testMeta = new TestInfo();

  @Test
  public void applicationTest() throws Exception
  {
    String gatewayConnectAddress = "localhost:9090";
    URI uri = URI.create("ws://" + gatewayConnectAddress + "/pubsub");

    AdsDimensionsDemo adsDemo = new AdsDimensionsDemo();
    adsDemo.inputOperator = new MockGenerator();

    Configuration conf = new Configuration(false);
    conf.addResource("META-INF/properties.xml");
    conf.set("dt.attr.GATEWAY_CONNECT_ADDRESS", gatewayConnectAddress);
    conf.set("dt.application.AdsDimensionsDemoGeneric.operator.InputGenerator.attr.PARTITIONER",
             "com.datatorrent.common.partitioner.StatelessPartitioner:1");
    conf.set("dt.application.AdsDimensionsDemoGeneric.operator.Store.fileStore.basePathPrefix",
             testMeta.getDir());

    LocalMode lma = LocalMode.newInstance();
    lma.prepareDAG(adsDemo, conf);
    lma.cloneDAG();
    LocalMode.Controller lc = lma.getController();
    lc.setHeartbeatMonitoringEnabled(false);
    lc.runAsync();

    String query = SchemaUtils.jarResourceFileToString("adsquery.json");

    PubSubWebSocketAppDataQuery pubSubInput = new PubSubWebSocketAppDataQuery();

    CollectorTestSink<Object> sink = new CollectorTestSink<Object>();
    TestUtils.setSink(pubSubInput.outputPort, sink);

    pubSubInput.setTopic("AdsQueryGenericResultDemo.1");
    pubSubInput.setUri(uri);
    pubSubInput.setup(null);
    pubSubInput.activate(null);

    PubSubWebSocketOutputOperator<String> pubSubOutput = new PubSubWebSocketOutputOperator<String>();
    pubSubOutput.setTopic("AdsQueryGenericDemo");
    pubSubOutput.setUri(uri);
    pubSubOutput.setup(null);

    pubSubOutput.beginWindow(0);
    pubSubInput.beginWindow(0);

    Thread.sleep(5000);

    pubSubOutput.input.put(query);

    Thread.sleep(5000);

    pubSubInput.outputPort.flush(Integer.MAX_VALUE);

    Assert.assertEquals(1, sink.collectedTuples.size());
    String resultJSON = sink.collectedTuples.get(0).toString();
    JSONObject result = new JSONObject(resultJSON);
    JSONArray array = result.getJSONArray("data");
    JSONObject val = array.getJSONObject(0);
    Assert.assertEquals(1, array.length());

    Assert.assertEquals("3.00", val.get("revenue:SUM"));
    Assert.assertEquals("5.00", val.get("cost:SUM"));
    Assert.assertEquals("10", val.get("clicks:SUM"));
    Assert.assertEquals("5", val.get("impressions:SUM"));

    pubSubInput.deactivate();

    pubSubOutput.teardown();
    pubSubInput.teardown();
  }

  private static final Logger LOG = LoggerFactory.getLogger(AdsDimensionsDemoTest.class);
}
