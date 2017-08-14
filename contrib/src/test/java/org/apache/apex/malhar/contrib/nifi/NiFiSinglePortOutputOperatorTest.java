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

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import org.apache.apex.malhar.contrib.nifi.mock.MockSiteToSiteClient;
import org.apache.apex.malhar.contrib.nifi.mock.MockTransaction;
import org.apache.apex.malhar.lib.wal.FSWindowDataManager;
import org.apache.apex.malhar.lib.wal.WindowDataManager;
import org.apache.commons.io.IOUtils;
import org.apache.nifi.remote.protocol.DataPacket;
import org.apache.nifi.util.file.FileUtils;

import com.datatorrent.api.Attribute;
import com.datatorrent.api.Context;
import com.datatorrent.api.DAG;

import static org.apache.apex.malhar.lib.helper.OperatorContextTestHelper.mockOperatorContext;

public class NiFiSinglePortOutputOperatorTest
{

  private Context.OperatorContext context;
  private WindowDataManager windowDataManager;
  private MockSiteToSiteClient.Builder stsBuilder;
  private NiFiDataPacketBuilder<String> dpBuilder;
  private NiFiSinglePortOutputOperator<String> operator;

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

    windowDataManager = new FSWindowDataManager();

    stsBuilder = new MockSiteToSiteClient.Builder();
    dpBuilder = new StringNiFiDataPacketBuilder();
    operator = new NiFiSinglePortOutputOperator(stsBuilder, dpBuilder, windowDataManager, 1);
  }

  @Test
  public void testTransactionPerTuple() throws IOException
  {
    operator.setup(context);

    // get the mock client which will capture each transactions
    final MockSiteToSiteClient mockClient = (MockSiteToSiteClient)operator.client;

    final String tuple1 = "tuple1";
    final String tuple2 = "tuple2";
    final String tuple3 = "tuple3";

    operator.beginWindow(1);

    operator.inputPort.process(tuple1);
    Assert.assertEquals(1, mockClient.getMockTransactions().size());

    operator.inputPort.process(tuple2);
    Assert.assertEquals(2, mockClient.getMockTransactions().size());

    operator.inputPort.process(tuple3);
    Assert.assertEquals(3, mockClient.getMockTransactions().size());

    operator.endNewWindow();
    Assert.assertEquals(3, mockClient.getMockTransactions().size());

    // verify we sent the correct content
    List<String> expectedContents = Arrays.asList(tuple1, tuple2, tuple3);
    List<MockTransaction> transactions = mockClient.getMockTransactions();

    verifyTransactions(expectedContents, transactions);
  }

  @Test
  public void testBatchSize() throws IOException
  {
    final int batchSize = 3;
    operator = new NiFiSinglePortOutputOperator(stsBuilder, dpBuilder, windowDataManager, batchSize);
    operator.setup(context);

    // get the mock client which will capture each transactions
    final MockSiteToSiteClient mockClient = (MockSiteToSiteClient)operator.client;

    final String tuple1 = "tuple1";
    final String tuple2 = "tuple2";
    final String tuple3 = "tuple3";
    final String tuple4 = "tuple4";
    final String tuple5 = "tuple5";

    operator.beginWindow(1);

    operator.inputPort.process(tuple1);
    Assert.assertEquals(0, mockClient.getMockTransactions().size());

    operator.inputPort.process(tuple2);
    Assert.assertEquals(0, mockClient.getMockTransactions().size());

    // should cause the port to flush and create a transaction
    operator.inputPort.process(tuple3);
    Assert.assertEquals(1, mockClient.getMockTransactions().size());

    operator.inputPort.process(tuple4);
    Assert.assertEquals(1, mockClient.getMockTransactions().size());

    operator.inputPort.process(tuple5);
    Assert.assertEquals(1, mockClient.getMockTransactions().size());

    // should flush tuples 4 and 5 and cause a new transaction
    operator.endNewWindow();
    Assert.assertEquals(2, mockClient.getMockTransactions().size());

    // verify we sent the correct content
    List<String> expectedContents = Arrays.asList(tuple1, tuple2, tuple3, tuple4, tuple5);
    List<MockTransaction> transactions = mockClient.getMockTransactions();

    verifyTransactions(expectedContents, transactions);
  }

  @Test
  public void testReplay() throws IOException
  {
    final String tuple1 = "tuple1";
    final String tuple2 = "tuple2";
    final String tuple3 = "tuple3";

    operator.setup(context);
    operator.beginWindow(1);
    operator.inputPort.process(tuple1);
    operator.inputPort.process(tuple2);
    operator.inputPort.process(tuple3);
    operator.endWindow();

    // get the mock client which will capture each transactions
    MockSiteToSiteClient mockClient = (MockSiteToSiteClient)operator.client;
    Assert.assertEquals(3, mockClient.getMockTransactions().size());

    // simulate replaying window #1
    operator.setup(context);
    operator.beginWindow(1);
    operator.inputPort.process(tuple1);
    operator.inputPort.process(tuple2);
    operator.inputPort.process(tuple3);
    operator.endWindow();

    // should not have created any transactions on the new client
    mockClient = (MockSiteToSiteClient)operator.client;
    Assert.assertEquals(0, mockClient.getMockTransactions().size());
  }


  private void verifyTransactions(List<String> expectedContents, List<MockTransaction> transactions) throws IOException
  {
    // convert all the data packets in the transactions to strings
    final List<String> dataPacketContents = new ArrayList<>();

    for (MockTransaction mockTransaction : transactions) {
      List<DataPacket> dps = mockTransaction.getSentDataPackets();
      Assert.assertTrue(dps.size() > 0);

      for (DataPacket dp : dps) {
        final String dpContent = IOUtils.toString(dp.getData());
        dataPacketContents.add(dpContent);
      }
    }

    // verify each expected piece of content is found in the data packet contents
    for (String expectedContent : expectedContents) {
      boolean found = false;
      for (String dataPacket : dataPacketContents) {
        if (dataPacket.equals(expectedContent)) {
          found = true;
          break;
        }
      }
      Assert.assertTrue(found);
    }
  }

  /**
   * A builder that can create a NiFiDataPacket from a string.
   */
  public static class StringNiFiDataPacketBuilder implements NiFiDataPacketBuilder<String>
  {
    @Override
    public NiFiDataPacket createNiFiDataPacket(String s)
    {
      return new StandardNiFiDataPacket(s.getBytes(StandardCharsets.UTF_8), new HashMap<String, String>());
    }
  }

}
