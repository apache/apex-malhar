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
package org.apache.apex.malhar.contrib.nifi;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.jetbrains.annotations.NotNull;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import org.apache.apex.malhar.contrib.nifi.mock.MockDataPacket;
import org.apache.apex.malhar.contrib.nifi.mock.MockSiteToSiteClient;
import org.apache.apex.malhar.lib.testbench.CollectorTestSink;
import org.apache.apex.malhar.lib.wal.FSWindowDataManager;
import org.apache.apex.malhar.lib.wal.WindowDataManager;
import org.apache.nifi.remote.protocol.DataPacket;
import org.apache.nifi.stream.io.ByteArrayInputStream;
import org.apache.nifi.util.file.FileUtils;

import com.datatorrent.api.Attribute;
import com.datatorrent.api.Context;
import com.datatorrent.api.DAG;

import static org.apache.apex.malhar.lib.helper.OperatorContextTestHelper.mockOperatorContext;

public class NiFiSinglePortInputOperatorTest
{

  private MockSiteToSiteClient.Builder builder;
  private CollectorTestSink<Object> sink;
  private Context.OperatorContext context;
  private WindowDataManager windowDataManager;
  private NiFiSinglePortInputOperator operator;

  @Before
  public void setup() throws IOException
  {
    final String windowDataDir = "target/" + this.getClass().getSimpleName();
    final File windowDataDirFile = new File(windowDataDir);
    if (windowDataDirFile.exists()) {
      FileUtils.deleteFile(windowDataDirFile, true);
    }

    Attribute.AttributeMap attributeMap = new Attribute.AttributeMap.DefaultAttributeMap();
    attributeMap.put(DAG.APPLICATION_PATH, windowDataDir);

    context = mockOperatorContext(12345, attributeMap);

    sink = new CollectorTestSink<>();
    builder = new MockSiteToSiteClient.Builder();
    windowDataManager = new FSWindowDataManager();

    operator = new NiFiSinglePortInputOperator(builder, windowDataManager);
    operator.outputPort.setSink(sink);
  }

  @After
  public void teardown()
  {
    if (operator != null) {
      operator.teardown();
    }
  }

  @Test
  public void testSimpleInput() throws IOException
  {
    // create some mock packets and queue them in the builder before running the operator
    final List<DataPacket> dataPackets = getDataPackets(4);
    builder.queue(dataPackets);

    operator.setup(context);
    operator.beginWindow(1);
    operator.emitTuples();
    operator.endWindow();

    Assert.assertEquals("Size of collected tuples should equal size of mock data packets",
        dataPackets.size(), sink.collectedTuples.size());

    operator.beginWindow(2);
    operator.emitTuples();
    operator.endWindow();

    Assert.assertEquals("Size of collected tuples should still equal size of mock data packets",
        dataPackets.size(), sink.collectedTuples.size());

    // verify that the collector sink got all the expected content
    List<String> expectedContents = Arrays.asList("content1", "content2", "content3", "content4");
    verifyContents(expectedContents, sink.collectedTuples);

    // reinitialize the data manager so it picks up the saved data
    windowDataManager.setup(context);

    // verify that all the data packets were saved for window #1
    List<StandardNiFiDataPacket> windowData = (List<StandardNiFiDataPacket>)windowDataManager.retrieve(1);
    Assert.assertNotNull("Should have recovered data", windowData);
    Assert.assertEquals("Size of recovered data should equal size of mock data packets",
        dataPackets.size(), windowData.size());
  }

  @Test
  public void testRecoveryAndIdempotency()
  {
    // create some mock packets and queue them in the builder before running the operator
    final List<DataPacket> dataPackets = getDataPackets(4);
    builder.queue(dataPackets);

    operator.setup(context);
    operator.beginWindow(1);
    operator.emitTuples();
    operator.endWindow();

    Assert.assertEquals("Size of collected tuples should equal size of mock data packets",
        dataPackets.size(), sink.collectedTuples.size());

    // simulate failure and then re-deployment of operator

    sink.collectedTuples.clear();
    Assert.assertEquals("Should not have collected tuples", 0, sink.collectedTuples.size());

    operator.setup(context);
    operator.beginWindow(1);
    operator.emitTuples();
    operator.endWindow();

    Assert.assertEquals("Size of collected tuples should equal size of mock data packets",
        dataPackets.size(), sink.collectedTuples.size());
  }

  @NotNull
  private List<DataPacket> getDataPackets(int size)
  {
    List<DataPacket> dataPackets = new ArrayList<>();

    for (int i = 1; i <= size; i++) {
      dataPackets.add(getDataPacket(String.valueOf(i)));
    }
    return dataPackets;
  }

  @NotNull
  private DataPacket getDataPacket(final String id)
  {
    Map<String, String> attrs = new HashMap<>();
    attrs.put("keyA", "valA");
    attrs.put("keyB", "valB");
    attrs.put("key" + id, "val" + id);

    byte[] content = ("content" + id).getBytes(StandardCharsets.UTF_8);
    ByteArrayInputStream in = new ByteArrayInputStream(content);

    return new MockDataPacket(attrs, in, content.length);
  }

  private void verifyContents(List<String> expectedContents, List<Object> tuples)
  {
    for (String expectedContent : expectedContents) {
      boolean found = false;

      for (Object obj : tuples) {
        if (obj instanceof NiFiDataPacket) {
          NiFiDataPacket dp = (NiFiDataPacket)obj;
          Assert.assertEquals(3, dp.getAttributes().size());

          String content = new String(dp.getContent(), StandardCharsets.UTF_8);
          if (content.equals(expectedContent)) {
            found = true;
            break;
          }
        }
      }

      Assert.assertTrue(found);
    }
  }

}
