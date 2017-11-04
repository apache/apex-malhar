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
package org.apache.apex.malhar.kudu.scanner;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import org.apache.apex.malhar.kudu.AbstractKuduInputOperator;
import org.apache.apex.malhar.kudu.InputOperatorControlTuple;
import org.apache.apex.malhar.kudu.sqltranslator.SQLToKuduPredicatesTranslator;

/**
 * <p>A scanner implementation that scans kudu tablet rows in an order which does not guarantee the same
 * sequence of tuples in case of restart after a crash/kill. The nature of unordered scans is because
 * of an eager approach to scan kudu tablets as all the thread pool based threads scan in parallel and hence
 * ordering cannot be guaranteed to the same for each run on the same data set.
 *
 * @since 3.8.0
 */
public class KuduPartitionRandomOrderScanner<T,C extends InputOperatorControlTuple>
    extends AbstractKuduPartitionScanner<T,C>
{

  public KuduPartitionRandomOrderScanner(AbstractKuduInputOperator<T,C> parentOperator)
  {
    super(parentOperator);
    threadPoolExecutorSize = parentOperator.getPartitionPieAssignment().size();
    initScannerCommons();
  }

  /***
   * Scans all the records for the given query by launching a parallel scan of all the tablets that can
   * serve the data for the given query.
   * @param parsedQuery Instance of the parser that parsed the incoming query.
   * @param setters Setters that are resolved for the POJO class that represents the Kudu row
   * @return Total number of rows scanned
   * @throws IOException When the Kudu connection cannot be closed after preparing the plan.
   */
  @Override
  public int scanAllRecords(SQLToKuduPredicatesTranslator parsedQuery, Map<String, Object> setters) throws IOException
  {
    int counterForMeta = 0;
    List<KuduPartitionScanAssignmentMeta> preparedScans = preparePlanForScanners(parsedQuery);
    for ( KuduPartitionScanAssignmentMeta aMeta : preparedScans) {
      KuduPartitionScannerCallable<T,C> aScanJobThread = new KuduPartitionScannerCallable<T,C>(parentOperator,aMeta,
          verifyConnectionStaleness(counterForMeta),setters,parsedQuery);
      counterForMeta += 1;
      kuduConsumerExecutor.submit(aScanJobThread);
    }
    return preparedScans.size();
  }
}
