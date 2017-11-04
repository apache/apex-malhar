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
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.apache.apex.malhar.kudu.AbstractKuduInputOperator;
import org.apache.apex.malhar.kudu.InputOperatorControlTuple;
import org.apache.apex.malhar.kudu.sqltranslator.SQLToKuduPredicatesTranslator;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * <p>A scanner implementation that scans kudu tablet rows in a consistent order. The ordered nature is guaranteed by
 *  the following aspects.</p>
 * <ol>
 *   <li>All scan tokens for a given query are always ordered</li>
 *   <li>A given operator always takes a select ordinal scan tokens for a given list of scan tokens as step 1 above</li>
 *   <li>The consistent order scanner always scans one tablet after another ( and hence less performant compared to
 *   that of the Random order scanner {@link KuduPartitionRandomOrderScanner}) </li>
 * </ol>
 *
 * @since 3.8.0
 */
public class KuduPartitionConsistentOrderScanner<T,C extends InputOperatorControlTuple>
    extends AbstractKuduPartitionScanner<T,C>
{

  public KuduPartitionConsistentOrderScanner(AbstractKuduInputOperator<T,C> parentOperator)
  {
    super(parentOperator);
    threadPoolExecutorSize = 1;
    initScannerCommons();
  }

  @Override
  public int scanAllRecords(SQLToKuduPredicatesTranslator parsedQuery,
      Map<String,Object> setters) throws IOException
  {
    List<KuduPartitionScanAssignmentMeta> plannedScansForthisQuery = preparePlanForScanners(parsedQuery);
    kuduConsumerExecutor.submit(new SequentialScannerThread(parsedQuery,setters,plannedScansForthisQuery));
    return plannedScansForthisQuery.size();
  }

  /**
   * A simple thread that ensures that all of the scan tokens are executed one after another. We need this thread
   * to not block the {@link KuduPartitionConsistentOrderScanner#scanAllRecords(SQLToKuduPredicatesTranslator, Map)} to
   * not get blocked as it is called during an emitTuple() call. The Future returns the number of records scanned.
   */
  public class SequentialScannerThread implements Callable<Long>
  {
    SQLToKuduPredicatesTranslator parsedQuery;

    Map<String,Object> settersForThisQuery;

    List<KuduPartitionScanAssignmentMeta> scansForThisQuery;

    ExecutorService executorServiceForSequentialScanner = Executors.newFixedThreadPool(1);

    public SequentialScannerThread(SQLToKuduPredicatesTranslator parsedQueryTree,Map<String,Object> setters,
        List<KuduPartitionScanAssignmentMeta> plannedScans)
    {
      checkNotNull(parsedQueryTree,"Parsed SQL expression cannot be null for scanner");
      checkNotNull(setters,"Setters cannot be null for the scanner thread");
      checkNotNull(plannedScans,"Planned scan segments cannot be null for scanner thread");
      parsedQuery = parsedQueryTree;
      settersForThisQuery = setters;
      scansForThisQuery = plannedScans;
    }

    @Override
    public Long call() throws Exception
    {
      long overallCount = 0;
      int counterForMeta = 0;
      for ( KuduPartitionScanAssignmentMeta aMeta : scansForThisQuery) {
        KuduPartitionScannerCallable<T,C> aScanJobThread = new KuduPartitionScannerCallable<T,C>(parentOperator,aMeta,
            verifyConnectionStaleness(counterForMeta),settersForThisQuery,parsedQuery);
        counterForMeta += 1;
        Future<Long> scanResult = executorServiceForSequentialScanner.submit(aScanJobThread);
        overallCount += scanResult.get(); // block till we complete this chunk;
      }
      executorServiceForSequentialScanner.shutdown(); // release resources
      return overallCount;
    }
  }
}
