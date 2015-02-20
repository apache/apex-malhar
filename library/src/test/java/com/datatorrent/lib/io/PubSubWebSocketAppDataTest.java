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
package com.datatorrent.lib.io;

import com.datatorrent.api.Context.OperatorContext;
import com.datatorrent.api.DAG;
import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.api.LocalMode;
import com.datatorrent.api.Operator;
import com.datatorrent.api.StreamingApplication;
import com.datatorrent.api.annotation.AppDataQueryPort;
import com.datatorrent.api.annotation.AppDataResultPort;
import com.datatorrent.api.annotation.OutputPortFieldAnnotation;
import com.datatorrent.lib.testbench.CollectorTestSink;
import com.google.common.collect.Maps;
import java.net.URI;

import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;
import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PubSubWebSocketAppDataTest
{
  private static final Logger logger = LoggerFactory.getLogger(PubSubWebSocketAppDataTest.class);

  @Test
  public void testPubSubWebsocketAppDataTest() throws Exception
  {
    final String topicName = "testTopic";
    final String testId1 = "1";
    final String testId2 = "2";

    /*Server server = new Server(0);
    SamplePubSubWebSocketServlet servlet = new SamplePubSubWebSocketServlet();
    ServletHolder sh = new ServletHolder(servlet);
    ServletContextHandler contextHandler = new ServletContextHandler(server, "/", ServletContextHandler.SESSIONS);
    contextHandler.addServlet(sh, "/pubsub");
    contextHandler.addServlet(sh, "/*");
    server.start();
    Connector connector[] = server.getConnectors();*/
    //URI uri = URI.create("ws://localhost:" + connector[0].getLocalPort() + "/pubsub");
    URI uri = URI.create("ws://localhost:9090/pubsub");
    logger.debug("Location of websocket: {}", uri);

    PubSubWebSocketOutputOperator<Map<String, String>> outputOperator = new PubSubWebSocketOutputOperator<Map<String, String>>();
    outputOperator.setName("testOutputOperator");
    outputOperator.setUri(uri);
    outputOperator.setTopic(topicName);

    PubSubWebSocketInputOperator<String> inputOperator1 = new PubSubWebSocketInputOperator<String>();
    inputOperator1.setName("testInputOperator1");
    inputOperator1.setUri(uri);
    inputOperator1.setTopic(topicName + testId1);

    PubSubWebSocketInputOperator<String> inputOperator2 = new PubSubWebSocketInputOperator<String>();
    inputOperator2.setName("testInputOperator2");
    inputOperator2.setUri(uri);
    inputOperator2.setTopic(topicName + testId2);

    CollectorTestSink<Object> sink1 = new CollectorTestSink<Object>();
    inputOperator1.outputPort.setSink(sink1);

    CollectorTestSink<Object> sink2 = new CollectorTestSink<Object>();
    inputOperator2.outputPort.setSink(sink2);

    LocalMode lma = LocalMode.newInstance();
    lma.prepareDAG(new PubSubWebSocketApplication(uri, topicName),
                   new Configuration());
    lma.cloneDAG(); // check serialization
    LocalMode.Controller lc = lma.getController();
    lc.setHeartbeatMonitoringEnabled(false);
    lc.runAsync();

    Thread.sleep(7000);

    inputOperator1.setup(null);
    inputOperator2.setup(null);
    outputOperator.setup(null);
    inputOperator1.activate(null);
    inputOperator2.activate(null);

    inputOperator1.beginWindow(0);
    inputOperator2.beginWindow(0);
    outputOperator.beginWindow(0);

    Thread.sleep(2000);

    Map<String, String> message1 = Maps.newHashMap();
    message1.put("id", testId1);
    message1.put("type", "garbageQuery");

    outputOperator.input.put(message1);

    Map<String, String> message2 = Maps.newHashMap();
    message2.put("id", testId2);
    message2.put("type", "garbageQuery");

    outputOperator.input.put(message2);

    outputOperator.endWindow();

    int timeoutMillis = 30000;

    while((sink1.collectedTuples.size() < 1 ||
           sink2.collectedTuples.size() < 1) &&
          timeoutMillis > 0) {
      inputOperator1.outputPort.flush(10);
      inputOperator2.outputPort.flush(10);
      timeoutMillis -= 20;
      Thread.sleep(20);
    }

    inputOperator1.endWindow();
    inputOperator2.endWindow();

    lc.shutdown();



    Assert.assertEquals("Expected one.", 1, sink1.collectedTuples.size());
    Assert.assertEquals("Expected one.", 1, sink2.collectedTuples.size());
  }

  public static class PubSubWebSocketApplication implements StreamingApplication
  {
    private URI uri;
    private String topic;

    public PubSubWebSocketApplication(URI uri, String topic)
    {
      this.uri = uri;
      this.topic = topic;
    }

    @Override
    public void populateDAG(DAG dag, Configuration conf)
    {
      PubSubWebSocketAppDataQuery appQuery = dag.addOperator("appQuery",
                                                             PubSubWebSocketAppDataQuery.class);
      appQuery.setName("appQuery");
      appQuery.setUri(uri);
      appQuery.setTopic(topic);

      PubSubWebSocketAppDataResult appResult = dag.addOperator("appResult",
                                                               PubSubWebSocketAppDataResult.class);
      appResult.setName("appResult");
      appResult.setUri(uri);
      appResult.setTopic(topic);

      ResultGenerator resultGenerator = dag.addOperator("resultGenerator",
                                                        ResultGenerator.class);


      dag.addStream("queryToResult", appQuery.outputPort, resultGenerator.input);
      dag.addStream("resultToAppResult", resultGenerator.output, appResult.input);
    }
  }

  public static class ResultGenerator implements Operator
  {
    private static final Logger logger = LoggerFactory.getLogger(ResultGenerator.class);

    @AppDataQueryPort()
    public final transient DefaultInputPort<String> input = new DefaultInputPort<String>()
    {
      private final ObjectMapper om = new ObjectMapper();

      @Override
      public void process(String tuple)
      {
        String id;

        try {
          JSONObject jo = new JSONObject(tuple);
          id = jo.getString("id");
        }
        catch(JSONException ex) {
          return;
        }

        Map<String, Object> message = Maps.newHashMap();
        message.put("id", id);
        message.put("type", "garbageInfo");
        message.put("data", "crap");

        JSONObject json = new JSONObject(message);

        logger.debug("json emission:{}", json);

        if(output.isConnected()) {
          output.emit(json.toString());
        }
      }
    };

    @AppDataResultPort(schemaType="garbageInfo", schemaVersion="1.0")
    @OutputPortFieldAnnotation(optional=true)
    public final transient DefaultOutputPort<String> output = new DefaultOutputPort<String>();

    public ResultGenerator()
    {
    }

    @Override
    public void setup(OperatorContext context)
    {
      //Do nothing
    }

    @Override
    public void beginWindow(long windowId)
    {
      //Do nothing
    }

    @Override
    public void endWindow()
    {
      //Do nothing
    }

    @Override
    public void teardown()
    {
      //Do nothing
    }
  }
}
