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
package org.apache.apex.malhar.kudu;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.kudu.client.ExternalConsistencyMode;
import org.apache.kudu.client.KuduClient;
import org.apache.kudu.client.KuduSession;
import org.apache.kudu.client.KuduTable;
import org.apache.kudu.client.SessionConfiguration;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

/**
 * <p>
 * Represents a connection to the Kudu cluster. An instance of this class is to
 * be supplied (via a builder pattern to) {@link AbstractKuduOutputOperator} to
 * connect to a Kudu cluster.
 * </p>
 *
 * @since 3.8.0
 */

public class ApexKuduConnection implements AutoCloseable, Serializable
{
  private static final long serialVersionUID = 4720185362997969198L;

  private transient  KuduSession kuduSession;

  private transient  KuduTable kuduTable;

  private transient  KuduClient kuduClient;

  public static final Logger LOG = LoggerFactory.getLogger(ApexKuduConnection.class);

  private ApexKuduConnectionBuilder builderForThisConnection;

  private ApexKuduConnection(ApexKuduConnectionBuilder builder)
  {
    checkNotNull(builder,"Builder cannot be null to establish kudu session");
    checkArgument(builder.mastersCollection.size() > 0, "Atleast one kudu master needs to be specified");
    checkNotNull(builder.tableName,"Kudu table cannot be null");
    builderForThisConnection = builder;
    KuduClient.KuduClientBuilder kuduClientBuilder = new KuduClient.KuduClientBuilder(builder.mastersCollection);
    if (builder.isOperationTimeOutSet) {
      kuduClientBuilder.defaultOperationTimeoutMs(builder.operationTimeOutMs);
    }
    if (builder.isBossThreadCountSet) {
      kuduClientBuilder.bossCount(builder.numBossThreads);
    }
    if (builder.isWorkerThreadsCountSet) {
      kuduClientBuilder.workerCount(builder.workerThreads);
    }
    if (builder.isSocketReadTimeOutSet) {
      kuduClientBuilder.defaultSocketReadTimeoutMs(builder.socketReadTimeOutMs);
    }
    kuduClient = kuduClientBuilder.build();
    kuduSession = kuduClient.newSession();
    if (builder.isFlushModeSet) {
      kuduSession.setFlushMode(builder.flushMode);
    }
    if (builder.isExternalConsistencyModeSet) {
      kuduSession.setExternalConsistencyMode(builder.externalConsistencyMode);
    }
    try {
      if (!kuduClient.tableExists(builder.tableName)) {
        throw new Exception("Table " + builder.tableName + " does not exist. ");
      } else {
        kuduTable = kuduClient.openTable(builder.tableName);
      }
    } catch (Exception e) {
      throw new RuntimeException("Kudu table existence could not be ascertained  " + e.getMessage(), e);
    }
  }

  @Override
  public void close() throws Exception
  {
    kuduSession.close();
    kuduClient.close();
  }

  public KuduSession getKuduSession()
  {
    return kuduSession;
  }

  public void setKuduSession(KuduSession kuduSession)
  {
    this.kuduSession = kuduSession;
  }

  public KuduTable getKuduTable()
  {
    return kuduTable;
  }

  public void setKuduTable(KuduTable kuduTable)
  {
    this.kuduTable = kuduTable;
  }

  public KuduClient getKuduClient()
  {
    return kuduClient;
  }

  public void setKuduClient(KuduClient kuduClient)
  {
    this.kuduClient = kuduClient;
  }

  public ApexKuduConnectionBuilder getBuilderForThisConnection()
  {
    return builderForThisConnection;
  }

  public void setBuilderForThisConnection(ApexKuduConnectionBuilder builderForThisConnection)
  {
    this.builderForThisConnection = builderForThisConnection;
  }

  public static class ApexKuduConnectionBuilder implements Serializable
  {
    private static final long serialVersionUID = -3428649955056723311L;

    List<String> mastersCollection = new ArrayList<>();

    String tableName;

    // optional props
    int numBossThreads = 1;

    boolean isBossThreadCountSet = false;

    int workerThreads = 2 *  Runtime.getRuntime().availableProcessors();

    boolean isWorkerThreadsCountSet = false;

    long socketReadTimeOutMs = 10000;

    boolean isSocketReadTimeOutSet = false;

    long operationTimeOutMs = 30000;

    boolean isOperationTimeOutSet = false;

    ExternalConsistencyMode externalConsistencyMode;

    boolean isExternalConsistencyModeSet = false;

    SessionConfiguration.FlushMode flushMode;

    boolean isFlushModeSet = false;

    public ApexKuduConnectionBuilder withTableName(String tableName)
    {
      this.tableName = tableName;
      return this;
    }

    public ApexKuduConnectionBuilder withAPossibleMasterHostAs(String masterHostAndPort)
    {
      mastersCollection.add(masterHostAndPort);
      return this;
    }

    public ApexKuduConnectionBuilder withNumberOfBossThreads(int numberOfBossThreads)
    {
      this.numBossThreads = numberOfBossThreads;
      isBossThreadCountSet = true;
      return this;
    }

    public ApexKuduConnectionBuilder withNumberOfWorkerThreads(int numberOfWorkerThreads)
    {
      this.workerThreads = numberOfWorkerThreads;
      isWorkerThreadsCountSet = true;
      return this;
    }

    public ApexKuduConnectionBuilder withSocketReadTimeOutAs(long socketReadTimeOut)
    {
      this.socketReadTimeOutMs = socketReadTimeOut;
      isSocketReadTimeOutSet = true;
      return this;
    }

    public ApexKuduConnectionBuilder withOperationTimeOutAs(long operationTimeOut)
    {
      this.operationTimeOutMs = operationTimeOut;
      isOperationTimeOutSet = true;
      return this;
    }

    public ApexKuduConnectionBuilder withExternalConsistencyMode(ExternalConsistencyMode externalConsistencyMode)
    {
      this.externalConsistencyMode = externalConsistencyMode;
      isExternalConsistencyModeSet = true;
      return this;
    }

    public ApexKuduConnectionBuilder withFlushMode(SessionConfiguration.FlushMode flushMode)
    {
      this.flushMode = flushMode;
      isFlushModeSet = true;
      return this;
    }

    public ApexKuduConnection build()
    {
      ApexKuduConnection apexKuduConnection = new ApexKuduConnection(this);
      return apexKuduConnection;
    }

  }
}
