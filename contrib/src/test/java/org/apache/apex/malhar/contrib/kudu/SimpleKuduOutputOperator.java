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
package org.apache.apex.malhar.contrib.kudu;

import org.apache.kudu.client.ExternalConsistencyMode;
import org.apache.kudu.client.SessionConfiguration;

public class SimpleKuduOutputOperator extends AbstractKuduOutputOperator
{
  @Override
  ApexKuduConnection.ApexKuduConnectionBuilder getKuduConnectionConfig()
  {
    return new ApexKuduConnection.ApexKuduConnectionBuilder()
        .withAPossibleMasterHostAs("localhost:7051")
        .withTableName("unittests")
        .withExternalConsistencyMode(ExternalConsistencyMode.COMMIT_WAIT)
        .withFlushMode(SessionConfiguration.FlushMode.AUTO_FLUSH_SYNC)
        .withNumberOfBossThreads(1)
        .withNumberOfWorkerThreads(2)
        .withSocketReadTimeOutAs(3000)
        .withOperationTimeOutAs(3000);
  }

  @Override
  protected boolean isEligibleForPassivationInReconcilingWindow(KuduExecutionContext executionContext,
      long reconcilingWindowId)
  {
    return true;
  }

  @Override
  protected Class getTuplePayloadClass()
  {
    return UnitTestTablePojo.class;
  }
}
