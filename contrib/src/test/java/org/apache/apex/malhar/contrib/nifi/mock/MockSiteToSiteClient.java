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
package org.apache.apex.malhar.contrib.nifi.mock;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

import org.apache.nifi.remote.Transaction;
import org.apache.nifi.remote.TransferDirection;
import org.apache.nifi.remote.client.SiteToSiteClient;
import org.apache.nifi.remote.client.SiteToSiteClientConfig;
import org.apache.nifi.remote.protocol.DataPacket;

public class MockSiteToSiteClient implements SiteToSiteClient
{

  private final SiteToSiteClientConfig config;
  private final List<DataPacket> queuedDataPackets;
  private final Iterator<DataPacket> iter;
  private final List<MockTransaction> transactions;

  public MockSiteToSiteClient(final MockSiteToSiteClient.Builder builder)
  {
    this.config = builder.buildConfig();
    this.queuedDataPackets = (builder.queuedDataPackets == null ?
        new ArrayList<DataPacket>() : builder.queuedDataPackets);
    this.iter = queuedDataPackets.iterator();
    this.transactions = new ArrayList<>();
  }

  @Override
  public Transaction createTransaction(TransferDirection direction) throws IOException
  {
    MockTransaction transaction = new MockTransaction(iter);
    transactions.add(transaction);
    return transaction;
  }

  @Override
  public boolean isSecure() throws IOException
  {
    return false;
  }

  @Override
  public SiteToSiteClientConfig getConfig()
  {
    return config;
  }

  @Override
  public void close() throws IOException
  {
    // nothing to do
  }

  public List<MockTransaction> getMockTransactions()
  {
    return Collections.unmodifiableList(transactions);
  }

  /**
   * A builder for MockSiteToSiteClients.
   */
  public static class Builder extends SiteToSiteClient.Builder
  {

    private List<DataPacket> queuedDataPackets;

    public MockSiteToSiteClient.Builder queue(List<DataPacket> queuedDataPackets)
    {
      this.queuedDataPackets = new ArrayList<>(queuedDataPackets);
      return this;
    }

    @Override
    public SiteToSiteClient build()
    {
      return new MockSiteToSiteClient(this);
    }
  }

}
