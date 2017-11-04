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
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.codehaus.jackson.annotate.JsonIgnore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.apex.malhar.kudu.AbstractKuduInputOperator;
import org.apache.apex.malhar.kudu.ApexKuduConnection;
import org.apache.apex.malhar.kudu.InputOperatorControlTuple;
import org.apache.apex.malhar.kudu.sqltranslator.KuduSQLParseTreeListener;
import org.apache.apex.malhar.kudu.sqltranslator.SQLToKuduPredicatesTranslator;
import org.apache.kudu.client.AsyncKuduScanner;
import org.apache.kudu.client.KuduPredicate;
import org.apache.kudu.client.KuduScanToken;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * An abstract class that contains logic common to all types of Scanner. A scanner is responsible for scanning rows from
 * the kudu table based on the incoming SQL query. See {@link KuduPartitionConsistentOrderScanner}
 *  and {@link KuduPartitionRandomOrderScanner} for options available as scanners.
 *
 * @since 3.8.0
 */
public abstract class AbstractKuduPartitionScanner<T,C extends InputOperatorControlTuple>
{
  @JsonIgnore
  AbstractKuduInputOperator<T,C> parentOperator;

  ExecutorService kuduConsumerExecutor;

  ApexKuduConnection.ApexKuduConnectionBuilder apexKuduConnectionInfo;

  int threadPoolExecutorSize = 1;

  Map<Integer, ApexKuduConnection> connectionPoolForThreads = new HashMap<>();

  private static final Logger LOG = LoggerFactory.getLogger(AbstractKuduPartitionScanner.class);

  /***
   * Scans all of the scans planned for the current input operator. Note that the current implementations are
   * threaded implementations. Depending on the scanner, there are multiple threads or single thread
   * @param parsedQuery Instance of the parser that parsed the incoming query.
   * @param setters Setters that are resolved for the POJO class that represents the Kudu row
   * @return The total number of records that have been scanned for the passed query
   * @throws IOException
   */
  public abstract int scanAllRecords(SQLToKuduPredicatesTranslator parsedQuery,
      Map<String,Object> setters) throws IOException;

  public AbstractKuduPartitionScanner(AbstractKuduInputOperator<T,C> parentOperatorRunningThisScanner)
  {
    parentOperator = parentOperatorRunningThisScanner;
    apexKuduConnectionInfo = parentOperator.getApexKuduConnectionInfo();
  }

  /***
   * Implements common initialization logic across all types of scanners. Currently it is 1. Creating the thread pool
   *  service for executors and 2. Creating the requisite connection pool for the scanners to use.
   */
  public void initScannerCommons()
  {
    kuduConsumerExecutor = Executors.newFixedThreadPool(threadPoolExecutorSize);
    List<KuduPartitionScanAssignmentMeta> allPartitionsThatNeedScan = parentOperator.getPartitionPieAssignment();
    Collections.sort(allPartitionsThatNeedScan,
        new Comparator<KuduPartitionScanAssignmentMeta>()
      {
        @Override
        public int compare(KuduPartitionScanAssignmentMeta left, KuduPartitionScanAssignmentMeta right)
        {
          return left.getOrdinal() - right.getOrdinal();
        }
      });
    for ( int i = 0; i < threadPoolExecutorSize; i++) {
      connectionPoolForThreads.put(i,apexKuduConnectionInfo.build());
    }
    LOG.info("Scanner running with " + connectionPoolForThreads.size() + " kudu connections");
  }

  /***
   * Used to renew a connection in case it is dead. There can be scenarios when there is a continuous sequence of
   * queries that do not touch a tablet resulting in inactivity on the kudu client session.
   * @param indexPos The index position in the connection pool
   * @return A renewed connection in case it was dead
   */
  public ApexKuduConnection verifyConnectionStaleness(int indexPos)
  {
    ApexKuduConnection apexKuduConnection = connectionPoolForThreads.get(indexPos);
    checkNotNull(apexKuduConnection, "Null connection not expected while checking staleness of" +
        " existing connection");
    if (apexKuduConnection.getKuduSession().isClosed()) {
      try {
        apexKuduConnection.close(); // closes the wrapper
      } catch (Exception e) {
        LOG.error(" Could not close a possibly stale kudu connection handle ", e);
      }
      LOG.info("Ripped the old kudu connection out and building a new connection for this scanner");
      ApexKuduConnection newConnection =  apexKuduConnection.getBuilderForThisConnection().build();
      connectionPoolForThreads.put(indexPos,newConnection);
      return newConnection;
    } else {
      return apexKuduConnection;
    }
  }

  /***
   * The main logic which takes the parsed in query and builds the Kudud scan tokens specific to this query.
   * It makes sure that these scan tokens are sorted before the actual scan tokens that are to be executed in the
   * current physical instance of the operator are shortlisted. Since the kudu scan taken builder gives the scan
   * tokens for the query and does not differentiate between a distributed system and a single instance system, this
   * method takes the plan as generated by the Kudu scan token builder and then chooses only those segments that were
   * decided to be the responsibility of this operator at partitioning time.
   * @param parsedQuery The parsed query instance
   * @return A list of partition scan metadata objects that are applicable for this instance of the physical operator
   * i.e. the operator owning this instance of the scanner.
   * @throws IOException If the scan assignment cannot be serialized
   */
  public List<KuduPartitionScanAssignmentMeta> preparePlanForScanners(SQLToKuduPredicatesTranslator parsedQuery)
    throws IOException
  {
    List<KuduPredicate> predicateList = parsedQuery.getKuduSQLParseTreeListener().getKuduPredicateList();
    ApexKuduConnection apexKuduConnection = verifyConnectionStaleness(0);// we will have atleast one connection
    KuduScanToken.KuduScanTokenBuilder builder = apexKuduConnection.getKuduClient().newScanTokenBuilder(
        apexKuduConnection.getKuduTable());
    builder = builder.setProjectedColumnNames(new ArrayList<>(
        parsedQuery.getKuduSQLParseTreeListener().getListOfColumnsUsed()));
    for (KuduPredicate aPredicate : predicateList) {
      builder = builder.addPredicate(aPredicate);
    }
    builder.setFaultTolerant(parentOperator.isFaultTolerantScanner());
    Map<String,String> optionsUsedForThisQuery = parentOperator.getOptionsEnabledForCurrentQuery();
    if ( optionsUsedForThisQuery.containsKey(KuduSQLParseTreeListener.READ_SNAPSHOT_TIME)) {
      try {
        long readSnapShotTime = Long.valueOf(optionsUsedForThisQuery.get(KuduSQLParseTreeListener.READ_SNAPSHOT_TIME));
        builder = builder.readMode(AsyncKuduScanner.ReadMode.READ_AT_SNAPSHOT);
        builder = builder.snapshotTimestampMicros(readSnapShotTime);
        LOG.info("Using read snapshot for this query as " + readSnapShotTime);
      } catch ( Exception ex) {
        LOG.error("Cannot parse the Read snaptshot time " + ex.getMessage(), ex);
      }
    }
    List<KuduScanToken> allPossibleScanTokens = builder.build();
    Collections.sort(allPossibleScanTokens, // Make sure we deal with a sorted list of scan tokens
        new Comparator<KuduScanToken>()
      {
        @Override
        public int compare(KuduScanToken left, KuduScanToken right)
        {
          return left.compareTo(right);
        }
      });
    LOG.info(" Query will scan " + allPossibleScanTokens.size() + " tablets");
    if ( LOG.isDebugEnabled()) {
      LOG.debug(" Predicates scheduled for this query are " + predicateList.size());
      for ( int i = 0; i < allPossibleScanTokens.size(); i++) {
        LOG.debug("A tablet scheduled for all operators scanning is " + allPossibleScanTokens.get(i).getTablet());
      }
    }
    List<KuduPartitionScanAssignmentMeta> partitionPieForThisOperator = parentOperator.getPartitionPieAssignment();
    List<KuduPartitionScanAssignmentMeta> returnOfAssignments = new ArrayList<>();
    int totalScansForThisQuery = allPossibleScanTokens.size();
    int counterForPartAssignments = 0;
    for (KuduPartitionScanAssignmentMeta aPartofThePie : partitionPieForThisOperator) {
      if ( aPartofThePie.getOrdinal() < totalScansForThisQuery) { // a given query plan might have less scantokens
        KuduPartitionScanAssignmentMeta aMetaForThisQuery = new KuduPartitionScanAssignmentMeta();
        aMetaForThisQuery.setTotalSize(totalScansForThisQuery);
        aMetaForThisQuery.setOrdinal(counterForPartAssignments);
        counterForPartAssignments += 1;
        aMetaForThisQuery.setCurrentQuery(parsedQuery.getSqlExpresssion());
        // we pick up only those ordinals that are part of the original partition pie assignment
        KuduScanToken aTokenForThisOperator = allPossibleScanTokens.get(aPartofThePie.getOrdinal());
        aMetaForThisQuery.setSerializedKuduScanToken(aTokenForThisOperator.serialize());
        returnOfAssignments.add(aMetaForThisQuery);
        LOG.debug("Added query scan for this operator " + aMetaForThisQuery + " with scan tablet as " +
            allPossibleScanTokens.get(aPartofThePie.getOrdinal()).getTablet());
      }
    }
    LOG.info(" A total of " + returnOfAssignments.size() + " have been scheduled for this operator");
    return returnOfAssignments;
  }

  /***
   * Closes the connection pool that is being maintained for each of the scanners.
   */
  public void close()
  {
    for ( int i = 0; i < connectionPoolForThreads.size(); i++) {
      try {
        connectionPoolForThreads.get(i).close();
      } catch (Exception e) {
        LOG.error("Error while closing kudu connection ",e);
      }
    }
    kuduConsumerExecutor.shutdown();
  }

  public AbstractKuduInputOperator<T, C> getParentOperator()
  {
    return parentOperator;
  }

  public void setParentOperator(AbstractKuduInputOperator<T, C> parentOperator)
  {
    this.parentOperator = parentOperator;
  }

  public Map<Integer, ApexKuduConnection> getConnectionPoolForThreads()
  {
    return connectionPoolForThreads;
  }

  public void setConnectionPoolForThreads(Map<Integer, ApexKuduConnection> connectionPoolForThreads)
  {
    this.connectionPoolForThreads = connectionPoolForThreads;
  }
}
