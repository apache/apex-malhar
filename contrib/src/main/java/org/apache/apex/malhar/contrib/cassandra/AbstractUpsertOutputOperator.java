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
package org.apache.apex.malhar.contrib.cassandra;

import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.Field;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;


import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.apex.malhar.lib.wal.FSWindowDataManager;
import org.apache.apex.malhar.lib.wal.WindowDataManager;
import org.apache.hadoop.classification.InterfaceStability;

import com.codahale.metrics.Timer;

import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.CodecRegistry;
import com.datastax.driver.core.ColumnMetadata;
import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.core.DataType;
import com.datastax.driver.core.Host;
import com.datastax.driver.core.KeyspaceMetadata;
import com.datastax.driver.core.Metrics;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.TableMetadata;
import com.datastax.driver.core.TypeCodec;



import com.datatorrent.api.AutoMetric;
import com.datatorrent.api.Context;
import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.api.Operator;
import com.datatorrent.api.annotation.InputPortFieldAnnotation;
import com.datatorrent.api.annotation.Stateless;
import com.datatorrent.common.util.BaseOperator;


import static com.google.common.base.Preconditions.checkNotNull;

/**
 * <p>An abstract operator that is used to mutate cassandra rows using PreparedStatements for faster executions.
 * It accommodates EXACTLY_ONCE Semantics if concrete implementations choose to implement an abstract method with
 * meaningful implementation. Cassandra not being a pure transactional database , the burden is on the concrete
 * implementation of the operator to handle these semantics for EXACTLY_ONCE scenarios.
 * It may also be noted that the transaction previously committed check is ONLY invoked
 * during the reconciliation window (and not for any other windows).</p>
 *
 * <p>
 * The typical implementation model is as follows :
 * <ol>
 *  <li> Create a concrete implementation of this class by extending this class and implementing a few methods.</li>
 *  <li> Define the payload ( a POJO ) that represents a Cassandra Row and is also part of this execution context </li>
 *     {@link UpsertExecutionContext}. The payload POJO is a template Parameter of this class
 *  <li> The Upstream operator that wants to write to Cassandra does the following </li> <ol type="a">
 *      <li> Create an instance of {@link UpsertExecutionContext} </li>
 *      <li> Set the payload ( an instance of the POJO created as step two above )</li>
 *      <li> Set additional execution context parameters like CollectionHandling style, List placement Styles </li>
 *         overriding TTLs, Update only if Primary keys exist and Consistency Levels etc. </ol>
 *
 *  <li> The concrete implementation would then execute this context as a cassandra row mutation </li>
 * </ol>
 * </p>
 *
 * <p>Please refer unit tests in the source repository for some concrete examples of usage.</p>
 *
 *
 * <p>
 * This operator supports the following features
 * <ol>
 * <li> The user need not surface multiple operator instances for multiple update patterns. The incoming POJO that
 * is part of the UpsertExecutionContext tuple is automatically interpreted along with the context to account for
 * the right Update Statement to be executed.</li>
 * <li>Highly customizable Connection policies. This is achieved by specifying the ConnectionStateManager.
 *    There are quite a few connection management aspects that can be
 *    controlled via {@link ConnectionStateManager} like consistency, load balancing, connection retries,
 *    table to use, keyspace to use etc. Please refer javadoc of {@link ConnectionStateManager} </li>
 * <li> Support for Collections : Map, List and Sets are supported
 *    User Defined types as part of collections are also supported. </li>
 * <li>  Support exists for both adding to an existing collection or removing entries from an existing collection.
 *    The POJO field that represents a collection is used to represent the collection that is to be added or removed
 *    from the database table.
 *    Thus this can be used to avoid a pattern of read and then write the final value into the cassandra column
 *    which can be used for low latency / high write pattern applications as we can avoid a read in the process. </li>
 * <li>  Supports List Placements : The execution context can be used to specify where the new incoming list
 *    is to be added ( in case there is an existing list in the current column of the current row being mutated.
 *    Supported options are APPEND or PREPEND to an existing list </li>
 * <li>  Support for User Defined Types. A pojo can have fields that represent the Cassandra Columns that are custom
 *    user defined types. Concrete implementations of the operator provide a mapping of the cassandra column name
 *    to the TypeCodec that is to be used for that field inside cassandra. Please refer javadoc of the method
 *    getCodecsForUserDefinedTypes() in this class for more details </li>
 * <li>  Support for custom mapping of POJO payload field names to that of cassandra columns. Practically speaking,
 *    POJO field names might not always match with Cassandra Column names and hence this support. This will also avoid
 *    writing a POJO just for the cassandra operator and thus an existing POJO can be passed around to this operator.
 *    Please refer javadoc {@link this.getPojoFieldNameToCassandraColumnNameOverride()} for an example </li>
 * <li>  TTL support - A default TTL can be set for the Connection ( via {@link ConnectionStateManager} ) and then used
 *    for all mutations. This TTL can further be overridden at a tuple execution level to accommodate use cases of
 *    setting custom column expiry typically useful in wide row implementations. </li>
 * <li>  Support for Counter Column tables. Counter tables are also supported with the values inside the incoming
 *    POJO added/subtracted from the counter column accordingly.Please note that the value is not an absolute overwrite.
 *    Rather the pojo column value represents the value that needs to be added to or subtracted from the current
 *    counter. </li>
 * <li>  Support for Composite Primary Keys is also supported. All the POJO fields that map to the composite
 *    primary key are used to resolve the primary key in case of a Composite Primary key table </li>
 * <li>  Support for conditional updates : This operator can be used as an Update Only operator as opposed to an
 *     Upsert operator. i.e. Update only IF EXISTS . This is achieved by setting the appropriate boolean in the
 *     {@link UpsertExecutionContext} tuple that is passed from the upstream operator.</li>
 * <li>  Lenient mapping of POJO fields to Cassandra column names. By default the POJO field names are case insensitive
 *     to cassandra column names. This can be further enhanced by over-riding mappings. Please refer feature 6 above.
 *     </li>
 * <li>  Defaults can be overridden at at tuple execution level for TTL & Consistency Policies </li>
 * <li>  Support for handling Nulls i.e. whether null values in the POJO are to be persisted as is or to be ignored so
 *     that the application need not perform a read to populate a POJO field if it is not available in the context</li>
 * <li>  A few autometrics are provided for monitoring the latency aspects of the cassandra cluster </li>
 * </ol>
 * </p>
 *
 * <b>Note that the operator also supports mutating only few columns of a given row (apart from the normal upsert use
 * cases) if required. This is achieved by setting the appropriate directives in the UpsertExecutionContext tuple</b>
 *
 * @since 3.6.0
 */

@InterfaceStability.Evolving
public abstract class AbstractUpsertOutputOperator extends BaseOperator implements
    Operator.ActivationListener<Context.OperatorContext>, Operator.CheckpointNotificationListener
{

  protected ConnectionStateManager connectionStateManager;

  private WindowDataManager windowDataManager;

  private long currentWindowId;

  private transient boolean isInSafeMode;

  private transient long reconcilingWindowId;

  private transient boolean isInReconcilingMode;

  protected transient Session session;

  protected transient Cluster cluster;

  transient Map<String, TypeCodec> complexTypeCodecs;

  transient Map<String, Class> userDefinedTypesClass;

  transient Map<String, DataType> columnDefinitions;

  transient Map<String, String> colNamesMap;

  transient Set<String> pkColumnNames;

  transient Set<String> counterColumns;

  transient Set<String> collectionColumns;

  transient Set<String> listColumns;

  transient Set<String> mapColumns;

  transient Set<String> setColumns;

  transient Set<String> userDefinedTypeColumns;

  transient Set<String> regularColumns;

  protected Map<String, Object> getters;

  protected Map<String, TypeCodec> codecsForCassandraColumnNames;

  CassandraPreparedStatementGenerator cassandraPreparedStatementGenerationUtil;

  transient Map<Long, PreparedStatement> preparedStatementTypes;
  transient Class<?> tuplePayloadClass;

  private static final transient Logger LOG = LoggerFactory.getLogger(AbstractUpsertOutputOperator.class);

  private static final String CASSANDRA_CONNECTION_PROPS_FILENAME = "CassandraOutputOperator.properties";

  // Metrics

  @AutoMetric
  transient long ignoredRequestsDuetoIfExistsCheck = 0;

  @AutoMetric
  transient long successfullWrites = 0;

  @AutoMetric
  long totalWriteRequestsAttempted = 0;

  @AutoMetric
  transient int numberOfHostsWrittenTo = 0;

  @AutoMetric
  transient double fifteenMinuteWriteRateLatency = 0.0;

  @AutoMetric
  transient double fiveMinuteWriteRateLatency = 0.0;

  @AutoMetric
  transient double oneMinuteWriteRateLatency = 0.0;

  @AutoMetric
  transient double meanWriteRateLatency = 0.0;

  @AutoMetric
  transient long totalIgnoresInThisWindow = 0;

  @AutoMetric
  long totalIgnoresSinceStart = 0;

  @AutoMetric
  transient long totalWriteTimeoutsInThisWindow = 0;

  @AutoMetric
  long totalWriteTimeoutsSinceStart = 0;

  @AutoMetric
  transient long totalWriteRetriesInThisWindow =  0;

  @AutoMetric
  long totalWriteRetriesSinceStart = 0;

  @AutoMetric
  transient long writesWithConsistencyOne = 0;

  @AutoMetric
  transient long writesWithConsistencyTwo = 0;

  @AutoMetric
  transient long writesWithConsistencyThree = 0;

  @AutoMetric
  transient long writesWithConsistencyAll = 0;

  @AutoMetric
  transient long writesWithConsistencyLocalOne = 0;

  @AutoMetric
  transient long writesWithConsistencyQuorum = 0;

  @AutoMetric
  transient long writesWithConsistencyLocalQuorum = 0;

  @AutoMetric
  transient long writeWithConsistencyLocalSerial = 0;

  @AutoMetric
  transient long writesWithConsistencyEachQuorum = 0;

  @AutoMetric
  transient long writesWithConsistencySerial = 0;

  @AutoMetric
  transient long writesWithConsistencyAny = 0;

  transient Set<Host> uniqueHostsWrittenToInCurrentWindow;


  @InputPortFieldAnnotation
  public final transient DefaultInputPort<UpsertExecutionContext> input = new DefaultInputPort<UpsertExecutionContext>()
  {
    @Override
    public void process(final UpsertExecutionContext tuple)
    {
      if (!isEligbleForPassivation(tuple)) {
        return;
      }
      BoundStatement stmnt = setDefaultsAndPrepareBoundStatement(tuple);
      ResultSet result = session.execute(stmnt);
      updatePerRowMetric(result);
    }
  }; // end of input port implementation

  @Override
  public void setup(Context.OperatorContext context)
  {
    super.setup(context);
    windowDataManager = getWindowDataManager();
    if ( windowDataManager == null) {
      windowDataManager = new FSWindowDataManager();
    }
    windowDataManager.setup(context);
  }

  @Override
  public void teardown()
  {
    super.teardown();
    if (null != windowDataManager) {
      windowDataManager.teardown();
    }
  }


  /**
   * Primarily resets the per window counter metrics.
   * @param windowId The windowid as provided by the apex framework
   */
  @Override
  public void beginWindow(long windowId)
  {
    super.beginWindow(windowId);
    totalIgnoresInThisWindow = 0;
    totalWriteTimeoutsInThisWindow = 0;
    totalWriteRetriesInThisWindow =  0;
    uniqueHostsWrittenToInCurrentWindow.clear();
    successfullWrites = 0;
    ignoredRequestsDuetoIfExistsCheck = 0;
    writesWithConsistencyOne = 0;
    writesWithConsistencyTwo = 0;
    writesWithConsistencyThree = 0;
    writesWithConsistencyAll = 0;
    writesWithConsistencyLocalOne = 0;
    writesWithConsistencyQuorum = 0;
    writesWithConsistencyLocalQuorum = 0;
    writeWithConsistencyLocalSerial = 0;
    writesWithConsistencyEachQuorum = 0;
    writesWithConsistencySerial = 0;
    writesWithConsistencyAny = 0;
    currentWindowId = windowId;
    if ( currentWindowId != Stateless.WINDOW_ID) {
      if (currentWindowId > reconcilingWindowId) {
        isInSafeMode = false;
        isInReconcilingMode = false;
      }
      if (currentWindowId == reconcilingWindowId) {
        isInReconcilingMode = true;
        isInSafeMode = false;
      }
      if (currentWindowId < reconcilingWindowId) {
        isInReconcilingMode = false;
        isInSafeMode = true;
      }
    }
  }

  /**
   * Builds the metrics that can be sent to Application master.
   * Note that some of the metrics are computed in the cassandra driver itself and hence just
   * extracted from the driver state itself.
   */
  @Override
  public void endWindow()
  {
    super.endWindow();
    Timer timerForThisWindow = session.getCluster().getMetrics().getRequestsTimer();
    totalWriteRequestsAttempted += timerForThisWindow.getCount();
    numberOfHostsWrittenTo = uniqueHostsWrittenToInCurrentWindow.size();
    fifteenMinuteWriteRateLatency = timerForThisWindow.getFifteenMinuteRate();
    fiveMinuteWriteRateLatency = timerForThisWindow.getFiveMinuteRate();
    oneMinuteWriteRateLatency = timerForThisWindow.getOneMinuteRate();
    meanWriteRateLatency = timerForThisWindow.getMeanRate();
    Metrics.Errors errors = session.getCluster().getMetrics().getErrorMetrics();
    totalIgnoresInThisWindow = errors.getIgnores().getCount() - totalIgnoresSinceStart;
    totalIgnoresSinceStart = errors.getIgnores().getCount();
    totalWriteTimeoutsInThisWindow = errors.getWriteTimeouts().getCount() - totalWriteTimeoutsSinceStart;
    totalWriteTimeoutsSinceStart = errors.getWriteTimeouts().getCount();
    totalWriteRetriesInThisWindow =  errors.getRetriesOnWriteTimeout().getCount() - totalWriteRetriesSinceStart;
    totalWriteRetriesSinceStart = errors.getRetriesOnWriteTimeout().getCount();
    try {
      // we do not need any particular state and hence reusing the window id itself
      windowDataManager.save(currentWindowId,currentWindowId);
    } catch (IOException e) {
      LOG.error("Error while persisting the current window state " + currentWindowId + " because " + e.getMessage());
      throw new RuntimeException(e.getMessage());
    }
  }

  /**
   * Initializes cassandra cluster connections as specified by the Connection State manager.
   * Aspects that are initialized here include Identifying primary key column names, non-primary key columns,
   * collection type columns, counter columns
   * It also queries the Keyspace and Table metadata for extracting the above information.
   * It finally prepares all possible prepared statements that can be executed in the lifetime of the operator
   * for various permutations like APPEND/REMOVE COLLECTIONS , LIST APPEND/PREPEND , set nulls, set TTLs etc
   * @param context The apex framework context
   */
  @Override
  public void activate(Context.OperatorContext context)
  {
    ConnectionStateManager.ConnectionBuilder connectionBuilder = withConnectionBuilder();
    if (connectionBuilder == null) {
      connectionBuilder = buildConnectionBuilderFromPropertiesFile();
    }
    checkNotNull(connectionBuilder, " Connection Builder cannot be null.");
    connectionStateManager = connectionBuilder.initialize();
    cluster = connectionStateManager.getCluster();
    session = connectionStateManager.getSession();
    checkNotNull(session, "Cassandra session cannot be null");
    tuplePayloadClass = getPayloadPojoClass();
    columnDefinitions = new HashMap<>();
    counterColumns = new HashSet<>();
    collectionColumns = new HashSet<>();
    pkColumnNames = new HashSet<>();
    listColumns = new HashSet<>();
    mapColumns = new HashSet<>();
    setColumns = new HashSet<>();
    codecsForCassandraColumnNames = new HashMap<>();
    userDefinedTypeColumns = new HashSet<>();
    regularColumns = new HashSet<>();
    colNamesMap = new HashMap<>();
    getters = new HashMap<>();
    userDefinedTypesClass = new HashMap<>();
    uniqueHostsWrittenToInCurrentWindow = new HashSet<>();
    registerCodecs();
    KeyspaceMetadata keyspaceMetadata = cluster.getMetadata()
        .getKeyspace(connectionStateManager.getKeyspaceName());
    TableMetadata tableMetadata = keyspaceMetadata
        .getTable(connectionStateManager.getTableName());
    registerPrimaryKeyColumnDefinitions(tableMetadata);
    registerNonPKColumnDefinitions(tableMetadata);
    preparedStatementTypes = new HashMap<>();
    generatePreparedStatements();
    registerGettersForPayload();
    isInSafeMode = false;
    isInReconcilingMode = false;
    reconcilingWindowId = Stateless.WINDOW_ID;
    if ( (context.getValue(Context.OperatorContext.ACTIVATION_WINDOW_ID) != Stateless.WINDOW_ID) &&
        context.getValue(Context.OperatorContext.ACTIVATION_WINDOW_ID) <
        windowDataManager.getLargestCompletedWindow()) {
      isInSafeMode = true;
      reconcilingWindowId = windowDataManager.getLargestCompletedWindow() + 1;
      isInReconcilingMode = false;
    }
  }

  @Override
  public void deactivate()
  {
    connectionStateManager.close();
  }


  @Override
  public void committed(long windowId)
  {
    try {
      windowDataManager.committed(windowId);
    } catch (IOException e) {
      LOG.error("Error while committing the window id " + windowId + " because " + e.getMessage());
      throw new RuntimeException(e.getMessage());
    }
  }

  @Override
  public void beforeCheckpoint(long windowId)
  {
    // nothing to be done here. Prevent concrete implementations to be forced to implement this
  }

  @Override
  public void checkpointed(long windowId)
  {
    // Nothing to be done here. Concrete operators can override if needed.
  }

  private ConnectionStateManager.ConnectionBuilder buildConnectionBuilderFromPropertiesFile()
  {
    ConnectionStateManager.ConnectionBuilder propFileBasedConnectionBuilder = null;
    Properties config = new Properties();
    try (InputStream cassandraPropsFile = getClass().getClassLoader().getResourceAsStream(
        CASSANDRA_CONNECTION_PROPS_FILENAME)) {
      config.load(cassandraPropsFile);
      propFileBasedConnectionBuilder = new ConnectionStateManager.ConnectionBuilder();
      return propFileBasedConnectionBuilder
          .withClusterNameAs(config.getProperty(ConnectionStateManager.ConnectionBuilder.CLUSTER_NAME_IN_PROPS_FILE))
          .withDCNameAs(config.getProperty(ConnectionStateManager.ConnectionBuilder.DC_NAME_IN_PROPS_FILE))
          .withKeySpaceNameAs(config.getProperty(ConnectionStateManager.ConnectionBuilder.KEYSPACE_NAME_IN_PROPS_FILE))
          .withTableNameAs(config.getProperty(ConnectionStateManager.ConnectionBuilder.TABLE_NAME_IN_PROPS_FILE))
          .withSeedNodes(config.getProperty(ConnectionStateManager.ConnectionBuilder.SEEDNODES_IN_PROPS_FILE));
    } catch (Exception ex) {
      LOG.error("Error while trying to load cassandra config from properties file " +
          CASSANDRA_CONNECTION_PROPS_FILENAME + " because " + ex.getMessage(), ex);
      return null;
    }
  }

  protected boolean isEligbleForPassivation(final UpsertExecutionContext tuple)
  {
    if (isInSafeMode) {
      return false;
    }
    if (isInReconcilingMode) {
      return reconcileRecord(tuple,currentWindowId);
    }
    return true;
  }

  private BoundStatement setDefaultsAndPrepareBoundStatement(UpsertExecutionContext tuple)
  {
    UpsertExecutionContext.NullHandlingMutationStyle nullHandlingMutationStyle = tuple.getNullHandlingMutationStyle();
    if (UpsertExecutionContext.NullHandlingMutationStyle.UNDEFINED == nullHandlingMutationStyle) {
      nullHandlingMutationStyle = UpsertExecutionContext.NullHandlingMutationStyle.SET_NULL_COLUMNS;
    }
    boolean setNulls = true;
    if (nullHandlingMutationStyle != UpsertExecutionContext.NullHandlingMutationStyle.SET_NULL_COLUMNS) {
      setNulls = false;
    }
    UpsertExecutionContext.CollectionMutationStyle collectionMutationStyle = tuple.getCollectionMutationStyle();
    if ((collectionMutationStyle == null) ||
        (collectionMutationStyle == UpsertExecutionContext.CollectionMutationStyle.UNDEFINED) ) {
      tuple.setCollectionMutationStyle(UpsertExecutionContext.CollectionMutationStyle.ADD_TO_EXISTING_COLLECTION);
    }
    UpsertExecutionContext.ListPlacementStyle listPlacementStyle = tuple.getListPlacementStyle();
    if ( (listPlacementStyle == null) || (listPlacementStyle == UpsertExecutionContext.ListPlacementStyle.UNDEFINED) ) {
      tuple.setListPlacementStyle(UpsertExecutionContext.ListPlacementStyle.APPEND_TO_EXISTING_LIST);
    }
    PreparedStatement preparedStatement = resolvePreparedStatementForCurrentExecutionContext(tuple);
    BoundStatement stmnt = processPayloadForExecution(preparedStatement, tuple, setNulls);
    if ((tuple.isTtlOverridden()) || (connectionStateManager.isTTLSet())) {
      int ttlToUse = connectionStateManager.getDefaultTtlInSecs();
      if (tuple.isTtlOverridden()) {
        ttlToUse = tuple.getOverridingTTL();
      }
      stmnt.setInt(CassandraPreparedStatementGenerator.TTL_PARAM_NAME, ttlToUse);
    }
    if (tuple.isOverridingConsistencyLevelSet()) {
      ConsistencyLevel currentConsistencyLevel = tuple.getOverridingConsistencyLevel();
      if (currentConsistencyLevel.isSerial()) {
        stmnt.setSerialConsistencyLevel(tuple.getOverridingConsistencyLevel());
      } else {
        stmnt.setConsistencyLevel(tuple.getOverridingConsistencyLevel());
      }
    }
    LOG.debug("Executing statement " + preparedStatement.getQueryString());
    return stmnt;
  }

  private void updatePerRowMetric(ResultSet result)
  {
    uniqueHostsWrittenToInCurrentWindow.add(result.getExecutionInfo().getQueriedHost());
    updateConsistencyLevelMetrics(result.getExecutionInfo().getAchievedConsistencyLevel());
    successfullWrites += 1;
    if (!result.wasApplied()) {
      ignoredRequestsDuetoIfExistsCheck += 1;
    }
  }

  private void updateConsistencyLevelMetrics(ConsistencyLevel resultConsistencyLevel)
  {
    if (resultConsistencyLevel == null) {
      return;
    }
    switch (resultConsistencyLevel) {
      case ALL:
        writesWithConsistencyAll += 1;
        break;
      case ANY:
        writesWithConsistencyAny += 1;
        break;
      case EACH_QUORUM:
        writesWithConsistencyEachQuorum += 1;
        break;
      case LOCAL_ONE:
        writesWithConsistencyLocalOne += 1;
        break;
      case LOCAL_QUORUM:
        writesWithConsistencyLocalQuorum += 1;
        break;
      case LOCAL_SERIAL:
        writeWithConsistencyLocalSerial += 1;
        break;
      case ONE:
        writesWithConsistencyOne += 1;
        break;
      case QUORUM:
        writesWithConsistencyQuorum += 1;
        break;
      case SERIAL:
        writesWithConsistencySerial += 1;
        break;
      case THREE:
        writesWithConsistencyThree += 1;
        break;
      case TWO:
        writesWithConsistencyTwo += 1;
        break;
      default:
        break;
    }
  }

  /**
   * Shortlists the prepared statement from a cache that is populated initially.
   * @param tuple The execution context that is used to mutate the current cassandra row
   * @return The prepared statement that is applicable for the current execution context
   */
  private PreparedStatement resolvePreparedStatementForCurrentExecutionContext(UpsertExecutionContext tuple)
  {
    EnumSet<OperationContext> operationContextValue = EnumSet.noneOf(OperationContext.class);

    UpsertExecutionContext.CollectionMutationStyle collectionMutationStyle = tuple.getCollectionMutationStyle();
    if (collectionMutationStyle != null) {
      if (collectionMutationStyle == UpsertExecutionContext.CollectionMutationStyle.ADD_TO_EXISTING_COLLECTION) {
        operationContextValue.add(OperationContext.COLLECTIONS_APPEND);
      }
      if (collectionMutationStyle == UpsertExecutionContext.CollectionMutationStyle.REMOVE_FROM_EXISTING_COLLECTION) {
        operationContextValue.add(OperationContext.COLLECTIONS_REMOVE);
      }
    }
    UpsertExecutionContext.ListPlacementStyle listPlacementStyle = tuple.getListPlacementStyle();
    boolean isListContextSet = false;
    if ((listPlacementStyle != null) && (collectionMutationStyle ==
        UpsertExecutionContext.CollectionMutationStyle.ADD_TO_EXISTING_COLLECTION)) {
      if (listPlacementStyle == UpsertExecutionContext.ListPlacementStyle.APPEND_TO_EXISTING_LIST) {
        operationContextValue.add(OperationContext.LIST_APPEND);
        isListContextSet = true;
      }
      if (listPlacementStyle == UpsertExecutionContext.ListPlacementStyle.PREPEND_TO_EXISTING_LIST) {
        operationContextValue.add(OperationContext.LIST_PREPEND);
        isListContextSet = true;
      }
    }
    if (!isListContextSet) {
      // use cases when remove is specified but we do not want to build separate prepared statments
      operationContextValue.add(OperationContext.LIST_APPEND);
    }
    if ((connectionStateManager.isTTLSet()) || (tuple.isTtlOverridden())) {
      operationContextValue.add(OperationContext.TTL_SET);
    } else {
      operationContextValue.add(OperationContext.TTL_NOT_SET);
    }
    if (tuple.isUpdateOnlyIfPrimaryKeyExists()) {
      operationContextValue.add(OperationContext.IF_EXISTS_CHECK_PRESENT);
    } else {
      operationContextValue.add(OperationContext.IF_EXISTS_CHECK_ABSENT);
    }
    return preparedStatementTypes.get(CassandraPreparedStatementGenerator
        .getSlotIndexForMutationContextPreparedStatement(operationContextValue));
  }

  /**
   * Generates a Boundstatement that can be executed for the given incoming tuple. This boundstatement is then
   * executed as a command
   * @param ps The prepared statement that was shortlisted to execute the given tuple
   * @param tuple The tuple that represents the current execution context
   * @param setNulls This represents the value whether the columns in the prepared statement need to be ignored or
   *                 considered
   * @return The boundstatement appropriately built
   */
  @SuppressWarnings(value = "unchecked")
  private BoundStatement processPayloadForExecution(final PreparedStatement ps, final UpsertExecutionContext tuple,
      final boolean setNulls)
  {
    BoundStatement boundStatement = ps.bind();
    Object pojoPayload = tuple.getPayload();
    for (String cassandraColName : getters.keySet()) {
      DataType dataType = columnDefinitions.get(cassandraColName);
      CassandraPojoUtils.populateBoundStatementWithValue(boundStatement,getters,dataType,cassandraColName,
          pojoPayload,setNulls,codecsForCassandraColumnNames);
    }
    return boundStatement;
  }


  /**
   * Builds th map that manages the getters for a given cassandra column
   * Aspects like case-insensitiveness , over-riding of column names to custom mappings
   */
  private void registerGettersForPayload()
  {
    Field[] classFields = tuplePayloadClass.getDeclaredFields();
    Set<String> allColNames = new HashSet<>();
    Map<String, DataType> dataTypeMap = new HashMap<>();
    Map<String,String> overridingColnamesMap = getPojoFieldNameToCassandraColumnNameOverride();
    if ( overridingColnamesMap == null) {
      overridingColnamesMap = new HashMap<>();
    }
    allColNames.addAll(pkColumnNames);
    allColNames.addAll(regularColumns);
    Set<String> normalizedColNames = new HashSet<>();
    Iterator<String> simpleColIterator = allColNames.iterator();
    while (simpleColIterator.hasNext()) {
      String aCol = simpleColIterator.next();
      normalizedColNames.add(aCol.toLowerCase());
      dataTypeMap.put(aCol.toLowerCase(), columnDefinitions.get(aCol));
      colNamesMap.put(aCol.toLowerCase(), aCol);
      codecsForCassandraColumnNames.put(aCol, complexTypeCodecs.get(aCol.toLowerCase()));
    }
    for (Field aField : classFields) {
      String aFieldName = aField.getName();
      if ( (normalizedColNames.contains(aFieldName.toLowerCase())) ||
          (overridingColnamesMap.containsKey(aFieldName)) ) {

        String getterExpr = aFieldName;
        DataType returnDataTypeOfGetter = dataTypeMap.get(aFieldName.toLowerCase());
        if (returnDataTypeOfGetter == null) {
          returnDataTypeOfGetter = dataTypeMap.get(overridingColnamesMap.get(aFieldName));
        }
        Object getter = CassandraPojoUtils.resolveGetterForField(tuplePayloadClass,getterExpr,
            returnDataTypeOfGetter,userDefinedTypesClass);
        String resolvedColumnName = colNamesMap.get(aFieldName.toLowerCase());
        if (overridingColnamesMap.containsKey(aFieldName)) {
          resolvedColumnName = overridingColnamesMap.get(aFieldName);
        }
        getters.put(resolvedColumnName, getter);
      }
    }
  }

  private void registerCodecs()
  {
    complexTypeCodecs = getCodecsForUserDefinedTypes();
    if (complexTypeCodecs != null) {
      CodecRegistry registry = cluster.getConfiguration().getCodecRegistry();
      if (cluster.getConfiguration().getProtocolOptions().getProtocolVersion().toInt() < 4) {
        LOG.error("Custom codecs are not supported for protocol version < 4");
        throw new RuntimeException("Custom codecs are not supported for protocol version < 4");
      }
      for (String typeCodecStr : complexTypeCodecs.keySet()) {
        TypeCodec codec = complexTypeCodecs.get(typeCodecStr);
        registry.register(codec);
        userDefinedTypesClass.put(typeCodecStr, codec.getJavaType().getRawType());
      }
    } else {
      complexTypeCodecs = new HashMap<>();
    }
  }

  private void registerNonPKColumnDefinitions(final TableMetadata tableMetadata)
  {
    List<ColumnMetadata> colInfoForTable = tableMetadata.getColumns();
    for (ColumnMetadata aColumnDefinition : colInfoForTable) {
      if (aColumnDefinition.getType().isCollection()) {
        collectionColumns.add(aColumnDefinition.getName());
      }
      if (!pkColumnNames.contains(aColumnDefinition.getName())) {
        columnDefinitions.put(aColumnDefinition.getName(), aColumnDefinition.getType());
        regularColumns.add(aColumnDefinition.getName());
      }
      parseForSpecialDataType(aColumnDefinition);
    }
  }

  private void parseForSpecialDataType(final ColumnMetadata aColumnDefinition)
  {
    switch (aColumnDefinition.getType().getName()) {
      case COUNTER:
        counterColumns.add(aColumnDefinition.getName());
        break;
      case MAP:
        mapColumns.add(aColumnDefinition.getName());
        break;
      case SET:
        setColumns.add(aColumnDefinition.getName());
        break;
      case LIST:
        listColumns.add(aColumnDefinition.getName());
        break;
      case UDT:
        userDefinedTypeColumns.add(aColumnDefinition.getName());
        break;
      default:
        break;
    }
  }

  private void registerPrimaryKeyColumnDefinitions(final TableMetadata tableMetadata)
  {
    List<ColumnMetadata> primaryKeyColumns = tableMetadata.getPrimaryKey();
    for (ColumnMetadata primaryColumn : primaryKeyColumns) {
      columnDefinitions.put(primaryColumn.getName(), primaryColumn.getType());
      pkColumnNames.add(primaryColumn.getName());
      parseForSpecialDataType(primaryColumn);
    }
  }

  private void generatePreparedStatements()
  {
    cassandraPreparedStatementGenerationUtil = new CassandraPreparedStatementGenerator(
        pkColumnNames, counterColumns, listColumns,
        mapColumns, setColumns, columnDefinitions);
    cassandraPreparedStatementGenerationUtil.generatePreparedStatements(session, preparedStatementTypes,
        connectionStateManager.getKeyspaceName(), connectionStateManager.getTableName());
  }

  public Map<String, DataType> getColumnDefinitions()
  {
    return columnDefinitions;
  }

  public void setColumnDefinitions(final Map<String, DataType> columnDefinitions)
  {
    this.columnDefinitions = columnDefinitions;
  }

  public Map<String, Class> getUserDefinedTypesClass()
  {
    return userDefinedTypesClass;
  }

  public void setUserDefinedTypesClass(final Map<String, Class> userDefinedTypesClass)
  {
    this.userDefinedTypesClass = userDefinedTypesClass;
  }

  public Set<String> getPkColumnNames()
  {
    return pkColumnNames;
  }

  public void setPkColumnNames(final Set<String> pkColumnNames)
  {
    this.pkColumnNames = pkColumnNames;
  }

  public Set<String> getCounterColumns()
  {
    return counterColumns;
  }

  public void setCounterColumns(final Set<String> counterColumns)
  {
    this.counterColumns = counterColumns;
  }

  public Set<String> getCollectionColumns()
  {
    return collectionColumns;
  }

  public void setCollectionColumns(final Set<String> collectionColumns)
  {
    this.collectionColumns = collectionColumns;
  }

  public Set<String> getListColumns()
  {
    return listColumns;
  }

  public void setListColumns(final Set<String> listColumns)
  {
    this.listColumns = listColumns;
  }

  public Set<String> getMapColumns()
  {
    return mapColumns;
  }

  public void setMapColumns(Set<String> mapColumns)
  {
    this.mapColumns = mapColumns;
  }

  public Set<String> getSetColumns()
  {
    return setColumns;
  }

  public void setSetColumns(Set<String> setColumns)
  {
    this.setColumns = setColumns;
  }

  public Set<String> getUserDefinedTypeColumns()
  {
    return userDefinedTypeColumns;
  }

  public void setUserDefinedTypeColumns(Set<String> userDefinedTypeColumns)
  {
    this.userDefinedTypeColumns = userDefinedTypeColumns;
  }

  public Set<String> getRegularColumns()
  {
    return regularColumns;
  }

  public void setRegularColumns(Set<String> regularColumns)
  {
    this.regularColumns = regularColumns;
  }

  public Map<Long, PreparedStatement> getPreparedStatementTypes()
  {
    return preparedStatementTypes;
  }

  public void setPreparedStatementTypes(Map<Long, PreparedStatement> preparedStatementTypes)
  {
    this.preparedStatementTypes = preparedStatementTypes;
  }

  public Map<String, Object> getGetters()
  {
    return getters;
  }

  public void setGetters(Map<String, Object> getters)
  {
    this.getters = getters;
  }

  public ConnectionStateManager getConnectionStateManager()
  {
    return connectionStateManager;
  }

  public void setConnectionStateManager(ConnectionStateManager connectionStateManager)
  {
    this.connectionStateManager = connectionStateManager;
  }

  public WindowDataManager getWindowDataManager()
  {
    return windowDataManager;
  }

  public void setWindowDataManager(WindowDataManager windowDataManager)
  {
    this.windowDataManager = windowDataManager;
  }

  /**
   * Implementing concrete Operator instances define the Connection Builder properties by implementing this method
   * Please refer to {@link org.apache.apex.malhar.contrib.cassandra.ConnectionStateManager.ConnectionBuilder} for
   * an example implementation of the ConnectionStateManager instantiation.
   * Note that if this method is returning null, the connection properties are
   * fetched from a properties file loaded from the classpath.
   * @return The connection state manager that is to be used for this Operator.
   */
  public ConnectionStateManager.ConnectionBuilder withConnectionBuilder()
  {
    return null;
  }

  /**
   * The implementing concrete operator needs to implement this map. The key is the name of the cassandra column
   * that this codec is used to map to. The TypeCode is used to represent the codec for that column in cassandra
   * Please refer to test example  UserUpsertOperator.java for implementation.
   * Concrete implementations can return a null if there are no user defined types
   * @return A map giving column name to the codec to be used
  */
  public abstract Map<String, TypeCodec> getCodecsForUserDefinedTypes();

  /**
   * Defines the Pojo class that is used to represent the row in the table that is set in the ConnectionStateManager
   * instance. The Class that is returned here should match the template type of the execution context
   * {@link UpsertExecutionContext}
   * @return The class that is used as the payload of the Execution context.
     */
  public abstract Class getPayloadPojoClass();

  /**
   * Concrete implementations can override this method to provide a custom map of a POJO file name to the cassandra
   * column name. This is useful when POJOs that are acting as payloads cannot comply with code conventions of POJO
   * as opposed to cassandra column names Ex: Cassandra column names might have underscores and POJO fields
   * might not be in that format. It may be noted case sensitivity is ignored when trying to match Cassandra
   * Column names in this operator. The following snippet provides an overview of the custom mapping that can be
   * done by the concrete operator implementation if required.
   * <p>
   *  <pre>
   *   protected Map<String, String> getPojoFieldNameToCassandraColumnNameOverride()
   *   {
   *     Map<String,String> overridingColMap = new HashMap<>();
   *     overridingColMap.put("topScores","top_scores"); // topScores is POJO field name and top_scores is Cassandra col
   *     return overridingColMap;
   *   }
   * </pre>
   * </p>
   * @return A map of the POJO field name as key and value as the Cassandra Column name
     */
  protected Map<String,String> getPojoFieldNameToCassandraColumnNameOverride()
  {
    return new HashMap<>();
  }

  /**
   *
   * Since Cassandra is not a strictly transactional system and if the Apex operator crashes when a window is in
   * transit, we might be replaying the same data to be written to cassandra. In the event of such situations, we
   * would like the control to the concrete operator instance to resolve if they want to let the write happen
   * or simply skip it. Return true if the write needs to go through or return false to prevent the write
   * from happening.
   * Note that this check only happens for one window of data when an operator is resuming from a previous start
   * In the case of a restart from a previously checkpointed window, the operator simply runs in a "safe mode"
   * until it reaches the reconciliation window. This is the only window in which this method is called. Hence it
   * might be okay if this method is "heavy". For example the implementor can choose to read from cassandra for the
   * incoming record key entry and decide to let the write through or ignore it completely. This is on a per tuple
   * basis just for the reconciliation window only.  Post reconciliation window, the data simply flows through
   * without this check.
   * @param T
   * @param windowId
   * @return Whether the current POJO that is being passed in should be allowed to write into the cassandra row just for
   * the reconciling window phase
   */
  abstract boolean reconcileRecord(Object T, long windowId);

  enum OperationContext
  {
    UNDEFINED,
    COLLECTIONS_APPEND,
    COLLECTIONS_REMOVE,
    LIST_APPEND,
    LIST_PREPEND,
    TTL_SET,
    TTL_NOT_SET,
    IF_EXISTS_CHECK_PRESENT,
    IF_EXISTS_CHECK_ABSENT,
  }
}
