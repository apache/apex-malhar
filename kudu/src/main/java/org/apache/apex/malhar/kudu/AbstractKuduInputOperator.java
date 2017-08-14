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

import java.io.IOException;
import java.lang.reflect.Field;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import javax.validation.constraints.NotNull;

import org.codehaus.jackson.annotate.JsonIgnore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.apex.api.ControlAwareDefaultOutputPort;
import org.apache.apex.malhar.kudu.partitioner.AbstractKuduInputPartitioner;
import org.apache.apex.malhar.kudu.partitioner.KuduOneToManyPartitioner;
import org.apache.apex.malhar.kudu.partitioner.KuduOneToOnePartitioner;
import org.apache.apex.malhar.kudu.partitioner.KuduPartitionScanStrategy;
import org.apache.apex.malhar.kudu.scanner.AbstractKuduPartitionScanner;
import org.apache.apex.malhar.kudu.scanner.KuduPartitionConsistentOrderScanner;
import org.apache.apex.malhar.kudu.scanner.KuduPartitionRandomOrderScanner;
import org.apache.apex.malhar.kudu.scanner.KuduPartitionScanAssignmentMeta;
import org.apache.apex.malhar.kudu.scanner.KuduRecordWithMeta;
import org.apache.apex.malhar.kudu.scanner.KuduScanOrderStrategy;
import org.apache.apex.malhar.kudu.sqltranslator.KuduSQLParseTreeListener;
import org.apache.apex.malhar.kudu.sqltranslator.SQLToKuduPredicatesTranslator;
import org.apache.apex.malhar.lib.util.PojoUtils;
import org.apache.apex.malhar.lib.wal.FSWindowDataManager;
import org.apache.apex.malhar.lib.wal.WindowDataManager;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.kudu.ColumnSchema;
import org.apache.kudu.Common;
import org.apache.kudu.client.KuduTable;

import com.conversantmedia.util.concurrent.DisruptorBlockingQueue;
import com.conversantmedia.util.concurrent.SpinPolicy;

import com.datatorrent.api.Context;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.api.InputOperator;
import com.datatorrent.api.Operator;
import com.datatorrent.api.Partitioner;
import com.datatorrent.api.StatsListener;
import com.datatorrent.api.annotation.Stateless;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * <p>Abstract Kudu Input Operator that can be used to stream POJO representation of a Kudu table row by providing a
 *  SQL expression as an input to the operator. The SQL expression is fed into the operator by implementing the
 *   {@link AbstractKuduInputOperator#getNextQuery()}. The query processing is a continuous process wherein the
 *    method is invoked as soon as the operator is done processing the current query</p>
 *
 * <p>
 *   The following are the main features of  the Kudu input operator
 *   <ol>
 *     <li>The scans are performed using a SQL expression. The SQL expression is limited to the constructs
 *      provided by the Kudu scan engine. As an example not equal to expressions are not allowed as predicates</li>
 *     <li>The SQL expression allows for scanning with options that allow for setting the read snap shot time
 *      and/or setting the message that will be sent as part of the control tuple after the query is sent to the
 *      downstream operators</li>
 *     <li>The operator allows for a custom control tuple to be sent when the query data is streamed to the downstream
 *      operators</li>
 *     <li>The operator is templatised.
 *       <ol>
 *         <li>The first template variable represents the POJO class that represents the single
 *      row of a kudu table</li>
 *        <li>The second template parameter represents the control tuple message that is sent to downstream operators
 *         once a query is complete.</li>
 *       </ol>
 *     </li>
 *     <li>
 *       The input operator can be configured as a fault tolerant input operator. Configuring for fault tolerance
 *       implies that the scan is slower but resilient for tablet server failures. See
 *       {@link AbstractKuduInputOperator#setFaultTolerantScanner(boolean)}</li>
 *     </li>
 *     <li>The number of partitions can be set via typical apex configuration or via the method
 *     {@link AbstractKuduInputOperator#setNumberOfPartitions(int)}. The configuration value is overridden by the
 *     API method if both are defined.</li>
 *     <li>
 *       A partition scan strategy can be configured. There are two types os supported scan partitioners.Default is
 *       MANY_TABLETS_PER_OPERATOR.
 *       <ol>
 *         <li>MANY_TABLETS_PER_OPERATOR: Many kudu tablets are mapped to one Apex parition while scanning. See
 *          {@link KuduOneToManyPartitioner}</li>
 *         <li>ONE_TABLET_PER_OPERATOR: One Kudu tablet to one Apex partition. See
 *          {@link KuduOneToOnePartitioner}</li>
 *       </ol>
 *     </li>
 *     <li>A scan order strategy can also be set. This configuration allows for two types of ordering. The default
 *     is RANDOM_ORDER_SCANNER.
 *      <ol>
 *        <li>CONSISTENT_ORDER_SCANNER: Ensures the scans are consistent across checkpoint and restarts. See
 *         {@link KuduPartitionConsistentOrderScanner}</li>
 *        <li>RANDOM_ORDER_SCANNER: Rows from different kudu tablets are streamed in parallel in different threads.
 *         Choose this option for the highest throughput but with the downside of no ordering guarantees across
 *          multiple launches of the operator. See {@link KuduPartitionRandomOrderScanner}</li>
 *      </ol>
 *     </li>
 *     <li>Reads from Kudu tablets happen on a different thread than the main operator thread. The operator uses the
 *      DisruptorBlockingQueue as the buffer to achieve optimal performance and high throughput. The data scanned is
 *      sent via this buffer to the kudu input operator. Depending on the configuration, there can be multiple threads
 *       scanning multiple kudu tablets in parallel. The input operator allows to fine tune the buffering strategy.
 *        Set the CPU policy to SPINNING to
 *     get the highest performance. However this setting results in showing a 100% CPU busy state on the host where
 *      it is spinning instead of doing any useful work until there is data processing required</li>
 *     <li>The Operator allows for exactly once semantics when resuming from a checkpoint after a crash recovery/
 *     restart. For the exactly once semantics, the operator developer who is extending the Abstract input
 *      operator will need to override the method
 *      {@link AbstractKuduInputOperator#isAllowedInReconcilingWindow(KuduRecordWithMeta)}. Return true if the
 *      row needs to be streamed downstream. Note that exactly once semantics requires a lot more
 *       complexity in a truly distributed framework. Note that this method is called only for one window when
 *        the operator is in a reconciling mode in the last window that it was operating in just before a crash
 *         or a shutdown of the application.</li>
 *     <li>The query processing is done by operator one query after another and is independent of the checkpoint
 *      boundary. This essentially means that any given instance of time all physical instances of the operator
 *       are not processing the same query. Downstream operators if using a unifier need to be aware of this. The
 *        control tuple sent downstream can be used to identify the boundaries of the operator query processing
 *         boundaries.
 *     {@link AbstractKuduInputOperator#getNextQuery()} method is used by the child classes of this operator to
 *      define the next SQL expression that needs to be processed.</li>
 *     <li>Connection to the Kudu cluster is managed by specifying a
 *      {@link ApexKuduConnection.ApexKuduConnectionBuilder)}. Use this builder to fine tune various connection configs
 *       to ensure that the optimal resources are set for the scanner</li>
 *     <li>The tuple throughput can be controlled by using the
 *      {@link AbstractKuduInputOperator#setMaxTuplesPerWindow(int)} to control the rate at which the
 *       tuples are emitted in any given window</li>
 *     <li>Note that the default window data manager is the FS based window data manger. Changing this to Noop or
 *      other dummy implementations would result in 'EXACTLY ONCE' semantics void.</li>
 *   </ol>
 * </p>
 * @since 3.8.0
 */
@InterfaceStability.Evolving
public abstract class AbstractKuduInputOperator<T,C extends InputOperatorControlTuple> implements InputOperator,
    Operator.ActivationListener<Context.OperatorContext>, Operator.CheckpointNotificationListener,
    Partitioner<AbstractKuduInputOperator>, StatsListener
{

  private static final Logger LOG = LoggerFactory.getLogger(AbstractKuduInputOperator.class);

  private KuduPartitionScanStrategy partitionScanStrategy = KuduPartitionScanStrategy.MANY_TABLETS_PER_OPERATOR;

  private KuduScanOrderStrategy scanOrderStrategy = KuduScanOrderStrategy.RANDOM_ORDER_SCANNER;

  protected ApexKuduConnection.ApexKuduConnectionBuilder apexKuduConnectionInfo;

  /** Represents the PIE of the total tablets that are distributed across all instances of the operator. Note that
   *  this <b>DOES NOT</b> represent the work distribution for a given query for reasons related to churn of
   *   partitioning. Any query that is processed is assigned is alloted a subset of this pie with the possibility of
   *   all the query utilizing all of the pie for computing the query.
  */
  private List<KuduPartitionScanAssignmentMeta> partitionPieAssignment = new ArrayList<>();

  protected Class<T> clazzForResultObject;

  private int numberOfPartitions = -1; // set to -1 to see in partition logic to see if user explicitly set this value

  protected String tableName;

  // performance fine tuning params

  /**
   *  Set this to false to increase throughput but at the risk of being Kudu tablet failure dependent
   */
  private boolean isFaultTolerantScanner = true;

  private SpinPolicy cpuSpinPolicyForWaitingInBuffer = SpinPolicy.WAITING;

  private int bufferCapacity = 8192;

  private int maxTuplesPerWindow = -1;


  // current query fields
  private String currentQueryBeingProcessed;

  private Map<String,String> optionsEnabledForCurrentQuery;

  /**
   * Used to track the number of scantokens that are valid for this operator instance and this query. Used
   * to track the current query processing status and whether the operator is ready to accept the next query.
   */
  private int plannedSegmentsForCurrentQuery = 0;

  /**
   * Used to track the completion status of each scantoken Kudu partition that was assigned for this query and this
   * physical instance of the operator.
   */
  private Map<KuduPartitionScanAssignmentMeta,Boolean> currentQueryCompletionStatus = new HashMap<>();

  private boolean allScansCompleteForThisQuery = true;

  private boolean isPartitioned = false;

  @NotNull
  private WindowDataManager windowDataManager = new FSWindowDataManager();

  private transient long currentWindowId;

  private transient long reconcilingPhaseWindowId;

  private transient boolean isCurrentlyInSafeMode;

  private transient boolean isCurrentlyInReconcilingMode;

  private transient Map<KuduPartitionScanAssignmentMeta,Long> windowManagerDataForScans;

  @JsonIgnore
  protected transient AbstractKuduInputPartitioner partitioner;

  @JsonIgnore
  protected transient AbstractKuduPartitionScanner<T,C> scanner;

  private transient DisruptorBlockingQueue<KuduRecordWithMeta<T>> buffer;

  private transient Map<String,String> kuduColNameToPOJOFieldNameMap;

  private transient  Map<String,ColumnSchema> kuduColNameToSchemaMapping;

  private transient int currentWindowTupleCount = 0;

  public final transient ControlAwareDefaultOutputPort<T> outputPort = new ControlAwareDefaultOutputPort<>();

  public final transient  DefaultOutputPort<String> errorPort = new DefaultOutputPort<>();

  public AbstractKuduInputOperator()
  {
    // Used by the Kryo cloning process.
    // Other usages would need to set the table name, class and Kudu connection using setters.
    // Kryo would take care of these values as it does a deep copy.
  }

  public AbstractKuduInputOperator(ApexKuduConnection.ApexKuduConnectionBuilder kuduConnectionInfo,
      Class<T> clazzForPOJO) throws Exception
  {
    checkNotNull(clazzForPOJO," Class definition for POJO cannot be null");
    checkNotNull(kuduConnectionInfo, "Kudu connection info cannot be null");
    apexKuduConnectionInfo = kuduConnectionInfo;
    clazzForResultObject = clazzForPOJO;
    tableName = kuduConnectionInfo.tableName;
  }

  /**
   * Implement this method to get the next query that needs to be processed by this instance of the operator. Note
   * that the given query is distributed across all physical instances of the operator. It is entirely possible that
   * each physical instance of the operator is processing a different query at any given instance of time because
   * the rows that need to be scanned might not be equally spread across all kudu partitions.
   * As long as all instances of the physical operator get the same sequence of queries as part of the implementation
   *  of this method, all the physical operator instances will ensure the same sequence of processing of the queries.
   * @return The next query string that needs to be processed.
   */
  protected abstract String getNextQuery();


  /**
  * Implements  the logic to process a given query. Note that this method is invoked from two states.
   *  One state when the Operator is resuming from a checkpointed state and the second when a new query needs to be
   *   processed as part of the normal execution processing.
  */
  protected boolean processForQueryString(String queryExpression)
  {
    LOG.info("Processing query " + queryExpression);
    SQLToKuduPredicatesTranslator parsedQuery = null;
    try {
      parsedQuery = new SQLToKuduPredicatesTranslator(
        queryExpression,new ArrayList<ColumnSchema>(kuduColNameToSchemaMapping.values()));
      parsedQuery.parseKuduExpression();
    } catch (Exception e) {
      LOG.error("Could not parse the SQL expression/query " + e.getMessage(),e);
      errorPort.emit(queryExpression);
      return false;
    }
    if (parsedQuery.getErrorListener().isSyntaxError()) {
      LOG.error(" Query is an invalid query " + queryExpression);
      errorPort.emit(queryExpression);
      return false;
    }
    if (!parsedQuery.getKuduSQLParseTreeListener().isSuccessfullyParsed()) {
      LOG.error(" Query could not be successfully parsed instead of being syntactically correct " + queryExpression);
      errorPort.emit(queryExpression);
      return false;
    }
    Map<String,Object> setters = extractSettersForResultObject(parsedQuery);
    try {
      currentQueryBeingProcessed = queryExpression;
      optionsEnabledForCurrentQuery.clear();
      optionsEnabledForCurrentQuery.putAll(parsedQuery.getKuduSQLParseTreeListener().getOptionsUsed());
      plannedSegmentsForCurrentQuery = scanner.scanAllRecords(parsedQuery,setters);
    } catch (IOException e) {
      LOG.error("Error while scanning the kudu segments " + e.getMessage(), e);
      errorPort.emit(queryExpression);
      return false;
    }
    LOG.info("Query" + queryExpression + " submitted for scanning");
    return true;
  }

  /**
  * Resets the processing state of data structures that are applicable at a query level.
  */
  protected boolean processForNextQuery()
  {
    LOG.info("Clearing data structures for processing query " + currentQueryBeingProcessed);
    windowManagerDataForScans.clear();
    currentQueryCompletionStatus.clear();
    return processForQueryString(getNextQuery());
  }


  /**
   * Scans the metadata for the kudu table that this operator is scanning for and
   * returns back the mapping for the kudu column name to the ColumnSchema metadata definition.
   * Note that the Kudu columns names are case sensitive.
   * @return A Map with Kudu column names as keys and value as the Column Definition.
   * @throws Exception
   */
  private Map<String,ColumnSchema> buildColumnSchemaForTable() throws Exception
  {
    if (kuduColNameToSchemaMapping == null) {
      ApexKuduConnection connectionForMetaDataScan = apexKuduConnectionInfo.build();
      KuduTable table = connectionForMetaDataScan.getKuduTable();
      List<ColumnSchema> tableColumns =  table.getSchema().getColumns();
      connectionForMetaDataScan.close();
      Map<String,ColumnSchema> columnSchemaMap = new HashMap<>();
      for (ColumnSchema aColumn: tableColumns) {
        columnSchemaMap.put(aColumn.getName(),aColumn);
      }
      kuduColNameToSchemaMapping = columnSchemaMap;
    }
    return kuduColNameToSchemaMapping;
  }

  /**
  * Method responsible for sending a control tuple whenever a new query is about to be processed.
  */
  protected void sendControlTupleForNewQuery()
  {
    C startNewQueryControlTuple = getControlTupleForNewQuery();
    if (startNewQueryControlTuple != null) {
      outputPort.emitControl(startNewQueryControlTuple);
    }
  }

  /**
   * Override this method to send a valid Control tuple to the downstream operator instances
   * @return The control tuple that needs to be sent at the beginning of the query
   */
  protected C getControlTupleForNewQuery()
  {
    return null;// default is null; Can be overridden in the child classes if custom representation is needed
  }

  /**
   * Fetches values form the buffer. This also checks for the limit set as the maximum tuples that can be
   * sent downstream in a single window.
   * @return Returns the next Kudu row along with its metadata that needs to be processed for filters
   */

  protected KuduRecordWithMeta<T> processNextTuple()
  {
    boolean emitTuple = true;
    KuduRecordWithMeta<T> entryFetchedFromBuffer = null;
    if (maxTuplesPerWindow != -1) {
      // we have an explicit upper window. Hence we need to check and then emit
      if (currentWindowTupleCount >= maxTuplesPerWindow) {
        emitTuple = false;
      }
    }
    if ( emitTuple) {
      try {
        entryFetchedFromBuffer = buffer.poll(100L, TimeUnit.MICROSECONDS);
      } catch (InterruptedException e) {
        LOG.debug("No entry available in the buffer " + e.getMessage(), e);
      }
    }
    return entryFetchedFromBuffer;
  }

  /***
   * Used to filter of tuples read from the Kudu scan if it is being replayed in the windows before the
   * reconciling window phase. THis method also invokes the filter check in case the current window is in the
   *  reconciling window phase
   * @param recordWithMeta
   */
  protected void filterTupleBasedOnCurrentState(KuduRecordWithMeta<T> recordWithMeta)
  {
    boolean filter = false;
    if (isCurrentlyInSafeMode) {
      filter = true;
    }
    KuduPartitionScanAssignmentMeta currentRecordMeta = recordWithMeta.getTabletMetadata();
    long currentPositionInScan = recordWithMeta.getPositionInScan();
    if (windowManagerDataForScans.containsKey(currentRecordMeta)) {
      long counterLimitForThisMeta = windowManagerDataForScans.get(currentRecordMeta);
      if ( currentPositionInScan <= counterLimitForThisMeta) {
        // This is the case of a replay and hence do not emit
        filter = true;
      } else {
        windowManagerDataForScans.put(currentRecordMeta,currentPositionInScan);
      }
    } else {
      windowManagerDataForScans.put(currentRecordMeta,currentPositionInScan);
    }
    if ( isCurrentlyInReconcilingMode) {
      // check to see if we can emit based on the buisness logic in a reconciling window processing state
      if ( !isAllowedInReconcilingWindow(recordWithMeta)) {
        filter = true;
      }
    }
    if (!filter) {
      outputPort.emit(recordWithMeta.getThePayload());
      currentWindowTupleCount += 1;
    }
  }

  /**
   * Sends either control tuples or data tuples basing on the values being fetched from the buffer.
   */
  @Override
  public void emitTuples()
  {
    if (allScansCompleteForThisQuery) {
      if (processForNextQuery() ) {
        sendControlTupleForNewQuery();
        allScansCompleteForThisQuery = false;
      }
      return;
    }
    KuduRecordWithMeta<T> entryFetchedFromBuffer = processNextTuple();
    if (entryFetchedFromBuffer != null) {
      if ( (!entryFetchedFromBuffer.isEndOfScanMarker()) &&
          (!entryFetchedFromBuffer.isBeginScanMarker())) {
        // we have a valid record
        filterTupleBasedOnCurrentState(entryFetchedFromBuffer);
        return;
      }
      if (entryFetchedFromBuffer.isEndOfScanMarker()) {
        processForEndScanMarker(entryFetchedFromBuffer);
        sendControlTupleForEndQuery();
      }
      if (entryFetchedFromBuffer.isBeginScanMarker()) {
        processForBeginScanMarker(entryFetchedFromBuffer);
      }
    }
  }

  /**
   * Sends a control tuple to the downstream operators. This method checks to see if the user wishes to send a custom
   *  control tuple. If yes, a control tuple that the user chooses to send is streamed to the downstream operators.
   *  See {@link AbstractKuduInputOperator#getControlTupleForEndQuery()}. If this is null but the user configured a
   *  control tuple to be sent as part of the SQL expression that is given as input, a control tuple will automatically
   *  be generated by this operator containing the message that the user set at the time of the Query definition.
   */
  protected void sendControlTupleForEndQuery()
  {
    if ( currentQueryBeingProcessed == null) {
      return; //
    }
    if (!allScansCompleteForThisQuery) {
      return;
    }
    C endControlTuple = getControlTupleForEndQuery();
    if (endControlTuple != null) {
      outputPort.emitControl(endControlTuple);
    } else {
      if (optionsEnabledForCurrentQuery.containsKey(KuduSQLParseTreeListener.CONTROLTUPLE_MESSAGE)) {
        InputOperatorControlTuple controlTuple = new InputOperatorControlTuple();
        controlTuple.setBeginNewQuery(false);
        controlTuple.setEndCurrentQuery(true);
        controlTuple.setQuery(currentQueryBeingProcessed);
        controlTuple.setControlMessage(
            optionsEnabledForCurrentQuery.get(KuduSQLParseTreeListener.CONTROLTUPLE_MESSAGE));
        outputPort.emitControl(controlTuple);
      }
    }
  }

  protected C getControlTupleForEndQuery()
  {
    return null;// default is null; Can be overridden in the child classes if custom representation is needed
  }

  protected void processForBeginScanMarker(KuduRecordWithMeta<T> entryFetchedFromBuffer)
  {
    currentQueryCompletionStatus.put(entryFetchedFromBuffer.getTabletMetadata(), false);
  }

  /**
   * Used to see if all the segments that are planned for the current query and the current physical instance of the
   * operator are done in terms of streaming tuples to the downstream operators.
   * @param entryFetchedFromBuffer
   */
  protected void processForEndScanMarker(KuduRecordWithMeta<T> entryFetchedFromBuffer)
  {
    Boolean currentStatus = currentQueryCompletionStatus.get(entryFetchedFromBuffer.getTabletMetadata());
    if (currentStatus == null) {
      LOG.error(" End scan marker cannot be precede a Begin Scan marker ");
    }
    currentQueryCompletionStatus.put(entryFetchedFromBuffer.getTabletMetadata(), true);
    if ( plannedSegmentsForCurrentQuery == 0 ) {
      allScansCompleteForThisQuery = true;
      return;
    }
    boolean areAllScansComplete  = true;
    if (currentQueryCompletionStatus.size() != plannedSegmentsForCurrentQuery) {
      return;
    }
    for (KuduPartitionScanAssignmentMeta aMeta: currentQueryCompletionStatus.keySet()) {
      if (!currentQueryCompletionStatus.get(aMeta)) {
        areAllScansComplete = false;
      }
    }
    if (areAllScansComplete) {
      allScansCompleteForThisQuery = true;
    }
  }

  /**
   * This method is used to give control the user of the operator to decide how to derive exactly once
   * semantics.  Note that this check is invoked in a reconciling window when the operator is resuming from a crash.
   * This check happens only in this window and in no other window.
   * @param extractedRecord
   * @return true if the tuple needs to be sent to downstream. False if one does not want to stream this tuple
   *  downstream. Note that this check is invoked in a reconciling window when the operator is resuming from a crash.
   */
  protected boolean isAllowedInReconcilingWindow(KuduRecordWithMeta<T> extractedRecord)
  {
    return true;
  }

  @Override
  public void beginWindow(long windowId)
  {
    currentWindowTupleCount = 0;
    currentWindowId = windowId;
    if ( currentWindowId != Stateless.WINDOW_ID) {
      if (currentWindowId > reconcilingPhaseWindowId) {
        isCurrentlyInSafeMode = false;
        isCurrentlyInReconcilingMode = false;
      }
      if (currentWindowId == reconcilingPhaseWindowId) {
        isCurrentlyInReconcilingMode = true;
        isCurrentlyInSafeMode = false;
      }
      if (currentWindowId < reconcilingPhaseWindowId) {
        isCurrentlyInReconcilingMode = false;
        isCurrentlyInSafeMode = true;
      }
    }
    LOG.info(" Current processing mode states Safe Mode = " + isCurrentlyInSafeMode + " Reconciling mode = "
        + isCurrentlyInReconcilingMode);
    LOG.info(" Current window ID = " + currentWindowId + " reconciling window ID = " + reconcilingPhaseWindowId);
  }

  @Override
  public void endWindow()
  {
    try {
      windowDataManager.save(windowManagerDataForScans,currentWindowId);
    } catch (IOException e) {
      throw new RuntimeException("Could not persist the window Data manager on end window boundary " +
          e.getMessage(),e);
    }
  }

  @Override
  public void setup(Context.OperatorContext context)
  {
    if (windowDataManager != null) {
      windowDataManager.setup(context);
    }
    try {
      buildColumnSchemaForTable();
    } catch (Exception e) {
      throw new RuntimeException("Error while trying to build the schema definition for the Kudu table " + tableName,
          e);
    }
    windowManagerDataForScans = new HashMap<>();
    optionsEnabledForCurrentQuery = new HashMap<>();
    initPartitioner();
    initBuffer();
    // Scanner can only be initialized after initializing the partitioner
    initScanner(); // note that this is not streaming any data yet. Only warming up the scanner ready to read data
    initCurrentState();
    isCurrentlyInSafeMode = false;
    isCurrentlyInReconcilingMode = false;
    reconcilingPhaseWindowId = Stateless.WINDOW_ID;
    if ( (context.getValue(Context.OperatorContext.ACTIVATION_WINDOW_ID) != Stateless.WINDOW_ID) &&
        context.getValue(Context.OperatorContext.ACTIVATION_WINDOW_ID) <
        windowDataManager.getLargestCompletedWindow()) {
      isCurrentlyInSafeMode = true;
      reconcilingPhaseWindowId = windowDataManager.getLargestCompletedWindow() + 1;
      LOG.info("Set reconciling window ID as " + reconcilingPhaseWindowId);
      isCurrentlyInReconcilingMode = false;
    }
  }

  @Override
  public void teardown()
  {
    scanner.close();
  }

  /**
   * Used to read an existing state from a window data manager and initialize the starting state of this operator. Note
   * that there might be a query that was partially processed before the checkpointing process or the crash was
   *  triggered.
   */
  private void initCurrentState()
  {
    Map<KuduPartitionScanAssignmentMeta,Long> savedState = null;
    if ( (windowManagerDataForScans.size() == 0) && (currentQueryBeingProcessed == null) ) {
      // This is the case of an application restart possibly and hence want to get the exact state
      try {
        savedState = (Map<KuduPartitionScanAssignmentMeta,Long>)windowDataManager.retrieve(
          windowDataManager.getLargestCompletedWindow());
      } catch (IOException e) {
        throw new RuntimeException("Error while retrieving the window manager data at the initialization phase", e);
      } catch ( NullPointerException ex) {
        LOG.error("Error while getting the window manager data ", ex);
      }
    }
    if ( ( savedState != null) && (savedState.size() > 0 ) ) {
      KuduPartitionScanAssignmentMeta aMeta = savedState.keySet().iterator().next(); // we have one atleast
      currentQueryBeingProcessed = aMeta.getCurrentQuery();
      allScansCompleteForThisQuery = false;
      windowManagerDataForScans.putAll(savedState);
      processForQueryString(currentQueryBeingProcessed);
    }
  }

  @Override
  public Collection<Partition<AbstractKuduInputOperator>> definePartitions(Collection collection,
      PartitioningContext context)
  {
    initPartitioner();
    return partitioner.definePartitions(collection,context);
  }

  @Override
  public void partitioned(Map partitions)
  {
    initPartitioner();
    partitioner.partitioned(partitions);
  }

  @Override
  public Response processStats(BatchedOperatorStats stats)
  {
    Response response = new Response();
    if ( ( partitionPieAssignment == null) || (partitionPieAssignment.size() == 0) ) {
      response.repartitionRequired = true;
    } else {
      response.repartitionRequired = false;
    }
    return response;
  }

  private void initBuffer()
  {
    buffer = new DisruptorBlockingQueue<KuduRecordWithMeta<T>>(bufferCapacity,cpuSpinPolicyForWaitingInBuffer);
  }

  private void initPartitioner()
  {
    if (partitioner != null) {
      return;
    }
    checkNotNull(apexKuduConnectionInfo,"Apex Kudu connection cannot be null while setting partitioner");
    switch (partitionScanStrategy) {
      case MANY_TABLETS_PER_OPERATOR:
        partitioner = new KuduOneToManyPartitioner(this);
        break;
      case ONE_TABLET_PER_OPERATOR:
      default:
        partitioner = new KuduOneToOnePartitioner(this);
        break;

    }
  }

  private void initScanner()
  {
    switch (scanOrderStrategy) {
      case CONSISTENT_ORDER_SCANNER:
        setFaultTolerantScanner(true); // Consistent order scanners require them to be fault tolerant
        scanner = new KuduPartitionConsistentOrderScanner<>(this);
        break;
      case RANDOM_ORDER_SCANNER:
      default:
        scanner = new KuduPartitionRandomOrderScanner(this);
        break;
    }
  }

  /***
   *
   * @param parsedQuery The parsed query string
   * @return Null if the SQL expression cannot be mapped properly to the POJO fields in which case the SQL string is
   *  sent to the Error port
   */
  public Map<String,Object> extractSettersForResultObject(SQLToKuduPredicatesTranslator parsedQuery)
  {
    Map<String,String> aliasesUsedForThisQuery = parsedQuery.getKuduSQLParseTreeListener().getAliases();
    Map<String,Object> setterMap = new HashMap<>();
    Field[] fieldsOfThisPojo = clazzForResultObject.getDeclaredFields();
    Set<String> allPojoFieldNamesUsed = new HashSet<>(aliasesUsedForThisQuery.values());
    for ( Field aField : fieldsOfThisPojo) {
      if (!allPojoFieldNamesUsed.contains(aField.getName())) {
        LOG.error("Invalid mapping fo Kudu table column name to the POJO field name " + aField.getName());
        return null; // SQL expression will be sent to the error port
      }
    }
    for (ColumnSchema aKuduTableColumn: kuduColNameToSchemaMapping.values()) {
      String kuduColumnName = aKuduTableColumn.getName();
      switch ( aKuduTableColumn.getType().getDataType().getNumber()) {
        case Common.DataType.BINARY_VALUE:
          setterMap.put(kuduColumnName,PojoUtils.createSetter(clazzForResultObject,
              aliasesUsedForThisQuery.get(kuduColumnName),ByteBuffer.class));
          break;
        case Common.DataType.BOOL_VALUE:
          setterMap.put(kuduColumnName,PojoUtils.createSetterBoolean(clazzForResultObject,
              aliasesUsedForThisQuery.get(kuduColumnName)));
          break;
        case Common.DataType.DOUBLE_VALUE:
          setterMap.put(kuduColumnName,PojoUtils.createSetterDouble(clazzForResultObject,
              aliasesUsedForThisQuery.get(kuduColumnName)));
          break;
        case Common.DataType.FLOAT_VALUE:
          setterMap.put(kuduColumnName,PojoUtils.createSetterFloat(clazzForResultObject,
              aliasesUsedForThisQuery.get(kuduColumnName)));
          break;
        case Common.DataType.INT8_VALUE:
          setterMap.put(kuduColumnName,PojoUtils.createSetterByte(clazzForResultObject,
              aliasesUsedForThisQuery.get(kuduColumnName)));
          break;
        case Common.DataType.INT16_VALUE:
          setterMap.put(kuduColumnName,PojoUtils.createSetterShort(clazzForResultObject,
              aliasesUsedForThisQuery.get(kuduColumnName)));
          break;
        case Common.DataType.INT32_VALUE:
          setterMap.put(kuduColumnName,PojoUtils.createSetterInt(clazzForResultObject,
              aliasesUsedForThisQuery.get(kuduColumnName)));
          break;
        case Common.DataType.UNIXTIME_MICROS_VALUE:
        case Common.DataType.INT64_VALUE:
          setterMap.put(kuduColumnName,PojoUtils.createSetterLong(clazzForResultObject,
              aliasesUsedForThisQuery.get(kuduColumnName)));
          break;
        case Common.DataType.STRING_VALUE:
          setterMap.put(kuduColumnName,PojoUtils.createSetter(clazzForResultObject,
              aliasesUsedForThisQuery.get(kuduColumnName),String.class));
          break;
        case Common.DataType.UINT8_VALUE:
          LOG.error("Unsigned int 8 not supported yet");
          throw new RuntimeException("uint8 not supported in Kudu schema yet");
        case Common.DataType.UINT16_VALUE:
          LOG.error("Unsigned int 16 not supported yet");
          throw new RuntimeException("uint16 not supported in Kudu schema yet");
        case Common.DataType.UINT32_VALUE:
          LOG.error("Unsigned int 32 not supported yet");
          throw new RuntimeException("uint32 not supported in Kudu schema yet");
        case Common.DataType.UINT64_VALUE:
          LOG.error("Unsigned int 64 not supported yet");
          throw new RuntimeException("uint64 not supported in Kudu schema yet");
        case Common.DataType.UNKNOWN_DATA_VALUE:
          LOG.error("unknown data type ( complex types ? )  not supported yet");
          throw new RuntimeException("Unknown data type  ( complex types ? ) not supported in Kudu schema yet");
        default:
          LOG.error("unknown type/default  ( complex types ? )  not supported yet");
          throw new RuntimeException("Unknown type/default  ( complex types ? ) not supported in Kudu schema yet");
      }
    }
    return setterMap;
  }

  @Override
  public void activate(Context.OperatorContext context)
  {
  }

  @Override
  public void deactivate()
  {

  }

  @Override
  public void beforeCheckpoint(long windowId)
  {

  }

  @Override
  public void checkpointed(long windowId)
  {

  }

  @Override
  public void committed(long windowId)
  {

  }

  public ApexKuduConnection.ApexKuduConnectionBuilder getApexKuduConnectionInfo()
  {
    return apexKuduConnectionInfo;
  }

  public void setApexKuduConnectionInfo(ApexKuduConnection.ApexKuduConnectionBuilder apexKuduConnection)
  {
    apexKuduConnectionInfo = apexKuduConnection;
  }

  public List<KuduPartitionScanAssignmentMeta> getPartitionPieAssignment()
  {
    return partitionPieAssignment;
  }

  public void setPartitionPieAssignment(List<KuduPartitionScanAssignmentMeta> partitionPieAssignment)
  {
    this.partitionPieAssignment = partitionPieAssignment;
  }

  public KuduPartitionScanStrategy getPartitionScanStrategy()
  {
    return partitionScanStrategy;
  }

  public void setPartitionScanStrategy(KuduPartitionScanStrategy partitionScanStrategy)
  {
    this.partitionScanStrategy = partitionScanStrategy;
  }

  public String getCurrentQueryBeingProcessed()
  {
    return currentQueryBeingProcessed;
  }

  public void setCurrentQueryBeingProcessed(String currentQueryBeingProcessed)
  {
    this.currentQueryBeingProcessed = currentQueryBeingProcessed;
  }

  public int getNumberOfPartitions()
  {
    return numberOfPartitions;
  }

  public void setNumberOfPartitions(int numberOfPartitions)
  {
    this.numberOfPartitions = numberOfPartitions;
  }

  public KuduScanOrderStrategy getScanOrderStrategy()
  {
    return scanOrderStrategy;
  }

  public void setScanOrderStrategy(KuduScanOrderStrategy scanOrderStrategy)
  {
    this.scanOrderStrategy = scanOrderStrategy;
  }

  public AbstractKuduInputPartitioner getPartitioner()
  {
    return partitioner;
  }

  public void setPartitioner(AbstractKuduInputPartitioner partitioner)
  {
    this.partitioner = partitioner;
  }

  public AbstractKuduPartitionScanner<T,C> getScanner()
  {
    return scanner;
  }

  public void setScanner(AbstractKuduPartitionScanner<T,C> scanner)
  {
    this.scanner = scanner;
  }

  public Class<T> getClazzForResultObject()
  {
    return clazzForResultObject;
  }

  public void setClazzForResultObject(Class<T> clazzForResultObject)
  {
    this.clazzForResultObject = clazzForResultObject;
  }

  public String getTableName()
  {
    return tableName;
  }

  public void setTableName(String tableName)
  {
    this.tableName = tableName;
  }

  public SpinPolicy getCpuSpinPolicyForWaitingInBuffer()
  {
    return cpuSpinPolicyForWaitingInBuffer;
  }

  public void setCpuSpinPolicyForWaitingInBuffer(SpinPolicy cpuSpinPolicyForWaitingInBuffer)
  {
    this.cpuSpinPolicyForWaitingInBuffer = cpuSpinPolicyForWaitingInBuffer;
  }

  public int getBufferCapacity()
  {
    return bufferCapacity;
  }

  public void setBufferCapacity(int bufferCapacity)
  {
    this.bufferCapacity = bufferCapacity;
  }

  public Map<String, String> getKuduColNameToPOJOFieldNameMap()
  {
    return kuduColNameToPOJOFieldNameMap;
  }

  public void setKuduColNameToPOJOFieldNameMap(Map<String, String> kuduColNameToPOJOFieldNameMap)
  {
    this.kuduColNameToPOJOFieldNameMap = kuduColNameToPOJOFieldNameMap;
  }

  public Map<String, ColumnSchema> getKuduColNameToSchemaMapping()
  {
    return kuduColNameToSchemaMapping;
  }

  public void setKuduColNameToSchemaMapping(Map<String, ColumnSchema> kuduColNameToSchemaMapping)
  {
    this.kuduColNameToSchemaMapping = kuduColNameToSchemaMapping;
  }

  public int getMaxTuplesPerWindow()
  {
    return maxTuplesPerWindow;
  }

  public void setMaxTuplesPerWindow(int maxTuplesPerWindow)
  {
    this.maxTuplesPerWindow = maxTuplesPerWindow;
  }

  /**
   * Note that it is recommended that the buffer capacity be in the powers of two
   * @param buffer
   */
  public void setBuffer(DisruptorBlockingQueue<KuduRecordWithMeta<T>> buffer)
  {
    this.buffer = buffer;
  }

  public DisruptorBlockingQueue<KuduRecordWithMeta<T>> getBuffer()
  {
    return buffer;
  }

  public WindowDataManager getWindowDataManager()
  {
    return windowDataManager;
  }

  public void setWindowDataManager(WindowDataManager windowDataManager)
  {
    this.windowDataManager = windowDataManager;
  }

  public boolean isFaultTolerantScanner()
  {
    return isFaultTolerantScanner;
  }

  public void setFaultTolerantScanner(boolean faultTolerantScanner)
  {
    isFaultTolerantScanner = faultTolerantScanner;
  }

  public Map<String, String> getOptionsEnabledForCurrentQuery()
  {
    return optionsEnabledForCurrentQuery;
  }

  public void setOptionsEnabledForCurrentQuery(Map<String, String> optionsEnabledForCurrentQuery)
  {
    this.optionsEnabledForCurrentQuery = optionsEnabledForCurrentQuery;
  }

  public boolean isPartitioned()
  {
    return isPartitioned;
  }

  public void setPartitioned(boolean partitioned)
  {
    isPartitioned = partitioned;
  }
}
