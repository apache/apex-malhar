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
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.apache.nifi.remote.Communicant;
import org.apache.nifi.remote.Transaction;
import org.apache.nifi.remote.TransactionCompletion;
import org.apache.nifi.remote.protocol.DataPacket;
import org.apache.nifi.stream.io.ByteArrayInputStream;

/**
 * A mock Transaction that will return the data passed in the constructor on calls to receive(),
 * and will store the data packets passed to send().
 */
public class MockTransaction implements Transaction
{

  private Iterator<DataPacket> dataPacketIter;

  private List<DataPacket> sentDataPackets = new ArrayList<>();

  public MockTransaction(Iterator<DataPacket> iter)
  {
    this.dataPacketIter = iter;
  }

  @Override
  public void send(DataPacket dataPacket) throws IOException
  {
    if (dataPacket != null) {
      this.sentDataPackets.add(dataPacket);
    }
  }

  @Override
  public void send(byte[] content, Map<String, String> attributes) throws IOException
  {
    this.sentDataPackets.add(new MockDataPacket(attributes, new ByteArrayInputStream(content), content.length));
  }

  public List<DataPacket> getSentDataPackets()
  {
    return Collections.unmodifiableList(sentDataPackets);
  }

  @Override
  public DataPacket receive() throws IOException
  {
    if (dataPacketIter != null && dataPacketIter.hasNext()) {
      return dataPacketIter.next();
    } else {
      return null;
    }
  }

  @Override
  public void confirm() throws IOException
  {

  }

  @Override
  public TransactionCompletion complete() throws IOException
  {
    return new TransactionCompletion()
    {
      @Override
      public boolean isBackoff()
      {
        return false;
      }

      @Override
      public int getDataPacketsTransferred()
      {
        return 0;
      }

      @Override
      public long getBytesTransferred()
      {
        return 0;
      }

      @Override
      public long getDuration(TimeUnit timeUnit)
      {
        return 0;
      }
    };
  }

  @Override
  public void cancel(String explanation) throws IOException
  {

  }

  @Override
  public void error()
  {

  }

  @Override
  public TransactionState getState() throws IOException
  {
    return TransactionState.TRANSACTION_COMPLETED;
  }

  @Override
  public Communicant getCommunicant()
  {
    return new Communicant()
    {
      @Override
      public String getUrl()
      {
        return null;
      }

      @Override
      public String getHost()
      {
        return null;
      }

      @Override
      public int getPort()
      {
        return 0;
      }

      @Override
      public String getDistinguishedName()
      {
        return null;
      }
    };
  }

}
